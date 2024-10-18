use auth_git2::GitAuthenticator;
use git2::Repository;

fn main() {
    let repo = Repository::open(std::env::current_dir().unwrap()).unwrap();
    let mut remote = repo.find_remote("origin").unwrap();
    let head = repo.head().unwrap().resolve().unwrap();
    let name = head.name().unwrap();
    if !name.starts_with("refs") {
        panic!("Invalid Name: {name}");
    }
    let auth = GitAuthenticator::default();
    auth.push(&repo, &mut remote, &[&format!("{name}:{name}")])
        .unwrap();
}
