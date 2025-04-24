use git2::Repository;
use std::fs::File;
use std::io::Write;

fn main() -> Result<(), git2::Error> {
    let repo = Repository::open(std::env::current_dir().unwrap())?;
    let _remote = repo.find_remote("origin")?; // Keep remote for potential future use, like fetching

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

    // 5. Create Output File
    let pack_file_path = format!("{}_vs_origin_{}.pack", local_branch_name, local_branch_name);
    let mut file = File::create(&pack_file_path)
        .map_err(|e| git2::Error::from_str(&format!("Failed to create pack file: {}", e)))?;

    // 6. Write Pack File using foreach to write to our file
    packbuilder.foreach(|data| {
        file.write_all(data).unwrap();
        true
    })?;

    println!("Successfully generated pack file: {}", pack_file_path);

    Ok(())
}
