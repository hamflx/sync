use git2::Repository;

fn main() {
    let repo = Repository::open(std::env::current_dir().unwrap()).unwrap();
    let remote = repo.find_remote("origin").unwrap();
    let head = repo.head().unwrap();

    let ref_head = head.resolve().unwrap();
    println!("ref: {:?}", ref_head.name().unwrap());

    let shorthand = head.shorthand().unwrap();
    println!("{shorthand}");

    println!("head: {:?}", head.is_branch());
    let target = head.symbolic_target().unwrap();
    println!("{target}");

    // remote.push(refspecs, opts)
}
