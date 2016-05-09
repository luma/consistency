extern crate consistency;
use std::fmt;
use std::cmp::Ordering;
use consistency::{Node, Ring};

// Create your node that will be hashed. You'll want to provide a way to uniquely identify
// the node. In this case we'll do it with an id property.
#[derive(Clone, Eq, Ord)]
struct TestNode {
  id: String,
  ip: &'static str,
  port: u32,
}

impl TestNode {
  fn new(id: &str, ip: &'static str, port: u32) -> TestNode {
    TestNode {
      id: id.to_string(),
      ip: ip,
      port: port,
    }
  }
}

// It must implement the consistency::Node trait
// Specifically this means that you'll need to implement the Clone, Display, Eq, and Ord traits and
// also a name method (fn name(&self) -> String).
impl Node for TestNode {
  fn name(&self) -> String {
    // id is our unique id, so we'll just return that.
    self.id.clone()
  }
}

impl PartialOrd for TestNode {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.name().cmp(&other.name()))
  }
}

impl PartialEq for TestNode {
  fn eq(&self, other: &Self) -> bool {
    self.name() == other.name()
  }
}

impl fmt::Display for TestNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestNode<({} addr={}:{})>", self.name(), self.ip, self.port)
    }
}


fn main() {
  let test_node1 = TestNode::new("Foo", "192.168.0.1", 1234);
  let test_node2 = TestNode::new("Bar", "192.168.0.2", 1234);
  let test_node3 = TestNode::new("Baz", "192.168.0.3", 1234);

  let mut ring = Ring::new(3, &test_node1);
  ring.add(&test_node2);
  ring.add(&test_node3);

  // The ring should now be partitioned into 9 vnodes that reference 3 nodes
  println!("\nOur ring is partitioned into {} vnodes that references {} nodes. It contains the following nodes:", ring.vnodes.len(), ring.nodes.len());
  for node in &ring.nodes {
    println!("\t{}", node);
  }

  println!("\nThe full ring is");
  for vnode in &ring.vnodes {
    println!("\t{} => Replica {} of {}", vnode.hash, vnode.replica, vnode.node);
  }


  println!("\nLet's look up some keys");
  for key in "where are all these nodes?".split_whitespace() {
    match ring.get(key) {
      Some(node) => println!("\tkey '{}' is on {}", key, node),
      None => println!("Couldn't find a node for that key. This should only happen if an error occurs :-("),
    }
  }

  println!("\n");
}
