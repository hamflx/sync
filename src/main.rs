use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key,
};
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;
use clap::{Parser, Subcommand};
use git2::{Buf, Repository, Signature};
use hostname;
use serde::Deserialize;
use tokio::runtime::Runtime;

// Include the credentials file directly at compile time
const CONFIG_TOML: &str = include_str!("cred.toml");
// Fixed encryption key for second round (32 bytes for AES-256)
const FIXED_KEY: &[u8; 32] = b"eZ4Ro3aish5zeitei!cau2aegei|Gh3a";

#[derive(Parser)]
#[command(name = "packer")]
#[command(about = "Git pack generator and uploader", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Upload a pack file with changes between local and remote branches
    Up {
        /// Upload raw pack file without encryption
        #[arg(long)]
        raw: bool,
    },
    /// Download and apply a pack file from remote storage
    Down,
    /// Upload a file to OSS and generate a download link
    S {
        /// Local file path to upload
        local_file: String,
        /// Remote object key (path in OSS)
        #[arg(required = false)]
        object_key: Option<String>,
    },
}

#[derive(Deserialize)]
struct Config {
    oss: OssConfig,
}

#[derive(Deserialize)]
struct OssConfig {
    #[serde(rename = "BucketName")]
    bucket_name: String,
    #[serde(rename = "Endpoint")]
    endpoint: String,
    #[serde(rename = "AccessKeyId")]
    access_key_id: String,
    #[serde(rename = "AccessKeySecret")]
    access_key_secret: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Up { raw } => cmd_up(*raw),
        Commands::Down => cmd_down(),
        Commands::S {
            local_file,
            object_key,
        } => {
            // If object_key is not provided, generate a default one
            let key = match object_key {
                Some(key) => key.clone(),
                None => {
                    let hostname = hostname::get()
                        .unwrap_or_else(|_| "unknown".into())
                        .to_string_lossy()
                        .to_string();

                    let file_name = std::path::Path::new(local_file)
                        .file_name()
                        .unwrap_or_else(|| std::ffi::OsStr::new("file"))
                        .to_string_lossy();

                    format!("from/{}/{}", hostname, file_name)
                }
            };

            cmd_s(local_file, &key)
        }
    }
}

fn cmd_up(raw: bool) -> Result<(), Box<dyn std::error::Error>> {
    // Parse config from the included string
    let config: Config = toml::from_str(CONFIG_TOML)?;

    let repo = Repository::open(std::env::current_dir().unwrap())?;

    // Get the current branch
    let head = repo.head()?;
    if !head.is_branch() {
        return Err(Box::new(git2::Error::from_str(
            "HEAD is not a branch (detached HEAD state)",
        )));
    }

    // Extract the branch name from the reference
    let branch_name = head
        .shorthand()
        .ok_or_else(|| git2::Error::from_str("Failed to get branch name from HEAD"))?;

    // Get the target commit id of the current branch
    let head_commit_oid = head
        .target()
        .ok_or_else(|| git2::Error::from_str("Branch reference is not a direct reference"))?;

    // Get the HEAD commit for parent reference
    let head_commit = repo.find_commit(head_commit_oid)?;

    // Create a tree from the index (staged changes)
    let mut index = repo.index()?;
    let staged_tree_oid = index.write_tree()?;
    let staged_tree = repo.find_tree(staged_tree_oid)?;

    // Create a temporary commit to represent the staged changes
    let signature = Signature::now("Git Pack Generator", "noreply@example.com")?;
    let message = "Temporary commit for pack generation";

    // Create a commit with the staged tree and the HEAD as parent
    let staged_commit_oid = repo.commit(
        None, // Don't update any references
        &signature,
        &signature,
        message,
        &staged_tree,
        &[&head_commit],
    )?;

    println!(
        "Created temporary commit for staged changes: {}",
        staged_commit_oid
    );

    // 2. Create and Configure Revwalk
    let mut revwalk = repo.revwalk()?;
    revwalk.push(staged_commit_oid)?; // Start from staged changes

    // Find the corresponding remote branch
    let remote_branch_name = format!("refs/remotes/origin/{}", branch_name);
    let remote_branch_exists = repo.find_reference(&remote_branch_name).is_ok();

    if remote_branch_exists {
        // If remote branch exists, only include commits not in the remote
        println!("Found remote branch: {}", remote_branch_name);
        let remote_branch_ref = repo.find_reference(&remote_branch_name)?;
        let remote_branch_oid = remote_branch_ref.target().ok_or_else(|| {
            git2::Error::from_str("Remote branch reference is not a direct reference")
        })?;
        revwalk.hide(remote_branch_oid)?; // Exclude commits reachable from origin/branch
    } else {
        // If remote branch doesn't exist, include all commits
        println!(
            "Remote branch not found: {}. Including all commits.",
            remote_branch_name
        );
        // We don't hide any commits in this case, so all commits will be included
    }

    revwalk.set_sorting(git2::Sort::TIME)?; // Optional: sort commits

    // 3. Create PackBuilder
    let mut packbuilder = repo.packbuilder()?;

    // 4. Insert Commits into PackBuilder - using insert_walk method
    packbuilder.insert_walk(&mut revwalk)?;

    // 5. Create a memory buffer for the pack data
    let mut buf = Buf::new();

    // 6. Write pack data directly to the buffer
    packbuilder.write_buf(&mut buf)?;

    // Extract the SHA string from the beginning of the pack data
    let staged_commit_sha = staged_commit_oid.to_string();

    // Get repository info to construct the pack filename
    let repo_info = extract_repo_info(&repo)?;

    // Generate a filename for the pack
    let pack_file_name = if raw {
        // For raw pack files: {repo_author}/{repo_name}/{branch_name}/head-{commit_sha}.pack
        format!(
            "{}/{}/{}/head-{}.pack",
            repo_info.author, repo_info.name, branch_name, staged_commit_sha
        )
    } else {
        // For encrypted pack files: {repo_author}/{repo_name}/{branch_name}/head.pack
        format!(
            "{}/{}/{}/head.pack",
            repo_info.author, repo_info.name, branch_name
        )
    };

    println!("Pack data generated, size: {} bytes", buf.len());
    println!("Using current branch: {}", branch_name);

    if raw {
        let pack_data = buf.to_vec();

        // Calculate human-readable size
        let size_str = if pack_data.len() < 1024 {
            format!("{} bytes", pack_data.len())
        } else if pack_data.len() < 1024 * 1024 {
            format!("{:.2} KB", pack_data.len() as f64 / 1024.0)
        } else {
            format!("{:.2} MB", pack_data.len() as f64 / (1024.0 * 1024.0))
        };

        // Upload the raw pack data to S3
        upload_pack_to_s3(&config.oss, &pack_file_name, pack_data)?;

        println!(
            "Raw pack data (size: {}) uploaded to S3 storage successfully as: {}",
            size_str, pack_file_name
        );

        // Generate a pre-signed URL for the uploaded file (expires in 1 hour)
        let presigned_url = generate_presigned_url(&config.oss, &pack_file_name, 3600 * 48)?;
        println!("Download URL (valid for 48 hours): {}", presigned_url);
    } else {
        // For encrypted pack files, prepend SHA and encrypt before uploading
        let mut pack_data_with_sha = staged_commit_sha.into_bytes();
        pack_data_with_sha.extend_from_slice(&buf.to_vec());

        // Encrypt the pack data using two-round AES encryption
        let encrypted_data = encrypt_pack_data(pack_data_with_sha)?;

        // Calculate human-readable size
        let size_str = if encrypted_data.len() < 1024 {
            format!("{} bytes", encrypted_data.len())
        } else if encrypted_data.len() < 1024 * 1024 {
            format!("{:.2} KB", encrypted_data.len() as f64 / 1024.0)
        } else {
            format!("{:.2} MB", encrypted_data.len() as f64 / (1024.0 * 1024.0))
        };

        // 7. Upload the encrypted pack data to S3
        upload_pack_to_s3(&config.oss, &pack_file_name, encrypted_data)?;

        println!(
            "Encrypted pack data (size: {}) uploaded to S3 storage successfully as: {}",
            size_str, pack_file_name
        );

        // Generate a pre-signed URL for the uploaded file (expires in 1 hour)
        let presigned_url = generate_presigned_url(&config.oss, &pack_file_name, 3600 * 48)?;
        println!("Download URL (valid for 48 hours): {}", presigned_url);
    }

    Ok(())
}

fn cmd_down() -> Result<(), Box<dyn std::error::Error>> {
    // Parse config from the included string
    let config: Config = toml::from_str(CONFIG_TOML)?;

    let repo = Repository::open(std::env::current_dir().unwrap())?;

    // Get the current branch
    let head = repo.head()?;
    if !head.is_branch() {
        return Err(Box::new(git2::Error::from_str(
            "HEAD is not a branch (detached HEAD state)",
        )));
    }

    // Extract the branch name from the reference
    let branch_name = head
        .shorthand()
        .ok_or_else(|| git2::Error::from_str("Failed to get branch name from HEAD"))?;

    // Get repository info to construct the pack filename
    let repo_info = extract_repo_info(&repo)?;

    // Generate a filename for the pack following the pattern: {repo_author}/{repo_name}/{branch_name}/head.pack
    let pack_file_name = format!(
        "{}/{}/{}/head.pack",
        repo_info.author, repo_info.name, branch_name
    );

    println!("Downloading pack file: {}", pack_file_name);

    // Download the encrypted pack data from S3
    let encrypted_data = download_pack_from_s3(&config.oss, &pack_file_name)?;

    // Decrypt the pack data
    let pack_data = decrypt_pack_data(encrypted_data)?;

    // Apply the pack to the repository
    apply_pack_to_repo(&repo, pack_data)?;

    println!("Pack file successfully applied to repository");

    Ok(())
}

fn cmd_s(local_file: &str, object_key: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Parse config from the included string
    let config: Config = toml::from_str(CONFIG_TOML)?;

    // Read the file
    let file_data = std::fs::read(local_file)?;

    // Calculate human-readable size
    let size_str = if file_data.len() < 1024 {
        format!("{} bytes", file_data.len())
    } else if file_data.len() < 1024 * 1024 {
        format!("{:.2} KB", file_data.len() as f64 / 1024.0)
    } else {
        format!("{:.2} MB", file_data.len() as f64 / (1024.0 * 1024.0))
    };

    println!("Uploading file: {} ({})", local_file, size_str);

    // Upload the file to S3
    upload_pack_to_s3(&config.oss, object_key, file_data)?;

    println!(
        "File uploaded to S3 storage successfully as: {}",
        object_key
    );

    // Generate a pre-signed URL for the uploaded file (expires in 48 hours)
    let presigned_url = generate_presigned_url(&config.oss, object_key, 3600 * 48)?;
    println!("Download URL (valid for 48 hours): {}", presigned_url);

    Ok(())
}

struct RepoInfo {
    author: String,
    name: String,
}

fn extract_repo_info(repo: &Repository) -> Result<RepoInfo, git2::Error> {
    // Try to get the origin remote
    let remote = match repo.find_remote("origin") {
        Ok(remote) => remote,
        Err(_) => {
            return Ok(RepoInfo {
                author: "unknown".to_string(),
                name: "unknown".to_string(),
            })
        }
    };

    // Get the URL of the origin remote
    let url = match remote.url() {
        Some(url) => url,
        None => {
            return Ok(RepoInfo {
                author: "unknown".to_string(),
                name: "unknown".to_string(),
            })
        }
    };

    // Parse the URL to extract author and repo name
    // Example URLs:
    // https://github.com/author/repo.git
    // git@github.com:author/repo.git

    let (author, name) = if url.contains("github.com") {
        if url.starts_with("git@") {
            // SSH format
            let parts: Vec<&str> = url.split(':').collect();
            if parts.len() >= 2 {
                let repo_part = parts[1].trim_end_matches(".git");
                let repo_parts: Vec<&str> = repo_part.split('/').collect();
                if repo_parts.len() >= 2 {
                    (repo_parts[0].to_string(), repo_parts[1].to_string())
                } else {
                    ("unknown".to_string(), repo_part.to_string())
                }
            } else {
                ("unknown".to_string(), "unknown".to_string())
            }
        } else {
            // HTTPS format
            let url_parts: Vec<&str> = url.split('/').collect();
            if url_parts.len() >= 5 {
                let author = url_parts[url_parts.len() - 2].to_string();
                let name = url_parts[url_parts.len() - 1]
                    .trim_end_matches(".git")
                    .to_string();
                (author, name)
            } else {
                ("unknown".to_string(), "unknown".to_string())
            }
        }
    } else {
        // Fallback for other Git hosting services
        let path_parts: Vec<&str> = url.split('/').collect();
        if path_parts.len() >= 2 {
            let name = path_parts[path_parts.len() - 1]
                .trim_end_matches(".git")
                .to_string();
            let author = path_parts[path_parts.len() - 2].to_string();
            (author, name)
        } else {
            ("unknown".to_string(), "unknown".to_string())
        }
    };

    Ok(RepoInfo { author, name })
}

fn upload_pack_to_s3(
    config: &OssConfig,
    file_name: &str,
    data: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create a tokio runtime for async operations
    let rt = Runtime::new()?;

    // Use the runtime to execute our async function
    rt.block_on(async {
        // Create S3 client with proper credentials
        let credentials_provider = aws_sdk_s3::config::Credentials::new(
            &config.access_key_id,
            &config.access_key_secret,
            None,
            None,
            "Static",
        );

        let region = Region::new("cn-beijing");
        let s3_config = aws_sdk_s3::Config::builder()
            .region(region)
            .endpoint_url(&config.endpoint)
            .credentials_provider(credentials_provider)
            .build();

        let client = Client::from_conf(s3_config);

        // Upload the data directly from memory
        let response = client
            .put_object()
            .bucket(&config.bucket_name)
            .key(file_name)
            .body(data.into())
            .send()
            .await?;

        println!("Upload response: {:?}", response);

        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

fn generate_presigned_url(
    config: &OssConfig,
    file_name: &str,
    expires_in_seconds: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    // Create a tokio runtime for async operations
    let rt = Runtime::new()?;

    // Use the runtime to execute our async function
    rt.block_on(async {
        // Create S3 client with proper credentials
        let credentials_provider = aws_sdk_s3::config::Credentials::new(
            &config.access_key_id,
            &config.access_key_secret,
            None,
            None,
            "Static",
        );

        let region = Region::new("cn-beijing");
        let s3_config = aws_sdk_s3::Config::builder()
            .region(region)
            .endpoint_url(&config.endpoint)
            .credentials_provider(credentials_provider)
            .build();

        // Create a presigner
        let presigning_config = aws_sdk_s3::presigning::PresigningConfig::builder()
            .expires_in(std::time::Duration::from_secs(expires_in_seconds))
            .build()?;

        let client = Client::from_conf(s3_config);

        // Generate a presigned URL for GetObject operation
        let presigned_request = client
            .get_object()
            .bucket(&config.bucket_name)
            .key(file_name)
            .presigned(presigning_config)
            .await?;

        Ok::<String, Box<dyn std::error::Error>>(presigned_request.uri().to_string())
    })
}

fn download_pack_from_s3(
    config: &OssConfig,
    file_name: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Create a tokio runtime for async operations
    let rt = Runtime::new()?;

    // Use the runtime to execute our async function
    rt.block_on(async {
        // Create S3 client with proper credentials
        let credentials_provider = aws_sdk_s3::config::Credentials::new(
            &config.access_key_id,
            &config.access_key_secret,
            None,
            None,
            "Static",
        );

        let region = Region::new("cn-beijing");
        let s3_config = aws_sdk_s3::Config::builder()
            .region(region)
            .endpoint_url(&config.endpoint)
            .credentials_provider(credentials_provider)
            .build();

        let client = Client::from_conf(s3_config);

        // Download the data
        let response = client
            .get_object()
            .bucket(&config.bucket_name)
            .key(file_name)
            .send()
            .await?;

        // Convert the response body to bytes
        let data = response.body.collect().await?.into_bytes().to_vec();

        println!("Downloaded encrypted pack file, size: {} bytes", data.len());

        Ok::<Vec<u8>, Box<dyn std::error::Error>>(data)
    })
}

fn encrypt_pack_data(pack_data: Vec<u8>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Generate a random key for first round encryption
    let random_key = Aes256Gcm::generate_key(OsRng);

    // First round encryption
    let cipher = Aes256Gcm::new(&random_key);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 96-bits; unique per message
    let first_round_encrypted = cipher
        .encrypt(&nonce, pack_data.as_ref())
        .map_err(|e| format!("First round encryption failed: {}", e))?;

    // Combine the encrypted data with the nonce and random key for second round
    let mut combined_data = Vec::new();
    combined_data.extend_from_slice(&nonce);
    combined_data.extend_from_slice(&random_key);
    combined_data.extend_from_slice(&first_round_encrypted);

    // Second round encryption with fixed key
    let fixed_key = Key::<Aes256Gcm>::from_slice(FIXED_KEY);
    let fixed_cipher = Aes256Gcm::new(fixed_key);
    let fixed_nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let second_round_encrypted = fixed_cipher
        .encrypt(&fixed_nonce, combined_data.as_ref())
        .map_err(|e| format!("Second round encryption failed: {}", e))?;

    // Prepend the fixed nonce to the final encrypted data
    let mut final_data = Vec::new();
    final_data.extend_from_slice(&fixed_nonce);
    final_data.extend_from_slice(&second_round_encrypted);

    println!(
        "Data encrypted successfully: {} bytes original → {} bytes encrypted",
        pack_data.len(),
        final_data.len()
    );

    Ok(final_data)
}

fn decrypt_pack_data(encrypted_data: Vec<u8>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // AES-GCM nonce size is 12 bytes
    const NONCE_SIZE: usize = 12;
    // AES-256 key size is 32 bytes
    const KEY_SIZE: usize = 32;

    if encrypted_data.len() <= NONCE_SIZE {
        return Err("Encrypted data too short".into());
    }

    // Extract the fixed nonce (first NONCE_SIZE bytes)
    let fixed_nonce = &encrypted_data[0..NONCE_SIZE];
    // The rest is the second round encrypted data
    let second_round_encrypted = &encrypted_data[NONCE_SIZE..];

    // Decrypt the second round with the fixed key
    let fixed_key = Key::<Aes256Gcm>::from_slice(FIXED_KEY);
    let fixed_cipher = Aes256Gcm::new(fixed_key);
    let combined_data = fixed_cipher
        .decrypt(fixed_nonce.into(), second_round_encrypted)
        .map_err(|e| format!("Second round decryption failed: {}", e))?;

    if combined_data.len() <= NONCE_SIZE + KEY_SIZE {
        return Err("Decrypted data from second round too short".into());
    }

    // Extract the components from the combined data
    let first_round_nonce = &combined_data[0..NONCE_SIZE];
    let random_key_bytes = &combined_data[NONCE_SIZE..(NONCE_SIZE + KEY_SIZE)];
    let first_round_encrypted = &combined_data[(NONCE_SIZE + KEY_SIZE)..];

    // Reconstruct the random key
    let random_key = Key::<Aes256Gcm>::from_slice(random_key_bytes);

    // Decrypt the first round with the random key
    let cipher = Aes256Gcm::new(random_key);
    let original_data = cipher
        .decrypt(first_round_nonce.into(), first_round_encrypted)
        .map_err(|e| format!("First round decryption failed: {}", e))?;

    println!(
        "Data decrypted successfully: {} bytes encrypted → {} bytes original",
        encrypted_data.len(),
        original_data.len()
    );

    Ok(original_data)
}

fn apply_pack_to_repo(
    repo: &Repository,
    pack_data: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Extract the SHA string from the beginning of the pack data
    // SHA is a 40 character hex string
    let sha_str = String::from_utf8_lossy(&pack_data[0..40]).to_string();
    let pack_data = &pack_data[40..]; // Remove the SHA from pack data

    // Create a temporary file to store the pack data
    let mut temp_file = tempfile::NamedTempFile::new()?;
    std::io::Write::write_all(&mut temp_file, pack_data)?;
    let temp_path = temp_file.path().to_str().unwrap();

    println!("Applying pack file to repository");
    println!("Using commit SHA: {}", sha_str);

    // Apply the pack to the repository's object database
    let output = std::process::Command::new("git")
        .args(&["index-pack", "--stdin", "--fix-thin"])
        .current_dir(repo.path().parent().unwrap_or(repo.path()))
        .stdin(std::process::Stdio::from(std::fs::File::open(temp_path)?))
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "Failed to apply pack: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    println!(
        "Pack applied to object database: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    // If we can't create a branch, just update the working directory with the changes
    let output = std::process::Command::new("git")
        .args(&["reset", "--hard", &sha_str])
        .current_dir(repo.path().parent().unwrap_or(repo.path()))
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "Failed to update working directory: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    Ok(())
}
