use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;
use git2::{Buf, Repository};
use serde::Deserialize;
use tokio::runtime::Runtime;

// Include the credentials file directly at compile time
const CONFIG_TOML: &str = include_str!("cred.toml");

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
    // Parse config from the included string
    let config: Config = toml::from_str(CONFIG_TOML)?;

    let repo = Repository::open(std::env::current_dir().unwrap())?;

    // 1. Find local branch - try dev, main, master in sequence
    let branch_names = ["dev", "main", "master"];
    let mut local_branch_ref = None;
    let mut local_branch_name = "";

    for branch_name in &branch_names {
        match repo.find_reference(&format!("refs/heads/{}", branch_name)) {
            Ok(reference) => {
                local_branch_ref = Some(reference);
                local_branch_name = branch_name;
                break;
            }
            Err(_) => continue,
        }
    }

    let local_branch_ref = local_branch_ref.ok_or_else(|| {
        git2::Error::from_str("None of the branches (dev, main, master) exist in this repository")
    })?;

    let local_branch_oid = local_branch_ref
        .target()
        .ok_or_else(|| git2::Error::from_str("Branch reference is not a direct reference"))?;

    // Find the corresponding remote branch
    let remote_branch_ref =
        repo.find_reference(&format!("refs/remotes/origin/{}", local_branch_name))?;
    let remote_branch_oid = remote_branch_ref.target().ok_or_else(|| {
        git2::Error::from_str("Remote branch reference is not a direct reference")
    })?;

    // 2. Create and Configure Revwalk
    let mut revwalk = repo.revwalk()?;
    revwalk.push(local_branch_oid)?; // Start from local branch
    revwalk.hide(remote_branch_oid)?; // Exclude commits reachable from origin/branch
    revwalk.set_sorting(git2::Sort::TIME)?; // Optional: sort commits

    // 3. Create PackBuilder
    let mut packbuilder = repo.packbuilder()?;

    // 4. Insert Commits into PackBuilder - using insert_walk method
    packbuilder.insert_walk(&mut revwalk)?;

    // 5. Create a memory buffer for the pack data
    let mut buf = Buf::new();

    // 6. Write pack data directly to the buffer
    packbuilder.write_buf(&mut buf)?;

    // Get repository info to construct the pack filename
    let repo_info = extract_repo_info(&repo)?;

    // Generate a filename for the pack following the pattern: {repo_author}/{repo_name}/{branch_name}/{origin_commit_hash}.pack
    let pack_file_name = format!(
        "{}/{}/{}/head.pack",
        repo_info.author, repo_info.name, local_branch_name
    );

    println!("Pack data generated, size: {} bytes", buf.len());

    // 7. Upload the pack data buffer directly to S3
    upload_pack_to_s3(&config.oss, &pack_file_name, buf.to_vec())?;

    println!(
        "Pack data uploaded to S3 storage successfully as: {}",
        pack_file_name
    );

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
