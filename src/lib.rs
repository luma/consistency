#![feature(plugin)]
#![plugin(clippy)]
#![feature(custom_derive)]
#[macro_use]
extern crate crypto;

use std::vec::Vec;
use std::boxed::Box;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Display;
use std::fmt::Debug;

use crypto::digest::Digest;
use crypto::sha1::Sha1;


/// Generate a Sha1 from a string key.
///
pub fn hash_key<'b, S: Into<&'b str>>(key: S) -> String {
  let mut hasher = Sha1::new();
  hasher.input_str(key.into());
  hasher.result_str()
}



pub trait Node: Clone + Debug + Display + Eq + Ord {
  fn name(&self) -> String;
}

#[derive(Debug, Eq)]
pub struct VNode <'a, N> where N: Node + 'a {
  pub replica: usize,
  pub node: &'a N,
  pub hash: String,
}

impl<'a, N: Node + 'a> VNode<'a, N> {
  pub fn new(replica: usize, node: &'a N) -> Self {
    let hash = hash_key(format!("{}_{}", node.name(), replica).as_str());

    VNode {
      replica: replica,
      node: node,
      hash: hash,
    }
  }
}

impl<'a, N: Node + 'a> PartialEq for VNode<'a, N> {
  fn eq(&self, other: &Self) -> bool {
    self.hash == other.hash
  }
}

impl<'a, N: Node + 'a> Ord for VNode<'a, N> {
  fn cmp(&self, other: &Self) -> Ordering {
    self.hash.cmp(&other.hash)
  }
}

impl<'a, N: Node + 'a> PartialOrd for VNode<'a, N> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<'a, N: Node + 'a> Display for VNode<'a, N> {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "VNode<({}:{}:{})>", self.node.name(), self.replica, self.hash)
  }
}

// TODO implement Cmp, etc for VNode

pub type VNodes<'a, N> = Vec<VNode<'a, N>>;

fn create_replicas_for_node<N: Node>(replicas: usize, node: &N) -> VNodes<N> {
  let mut vnodes: VNodes<_> = (0..replicas).map(|r| VNode::new(r, node))
                                           .collect();
  vnodes.sort();
  vnodes
}

pub struct Ring <'a, N> where N: Node + 'a {
  pub replicas: usize,
  pub nodes: Vec<Box<N>>,
  pub vnodes: VNodes<'a, N>,
}

impl<'a, N: Node + 'a> Ring<'a, N> {
  pub fn new(replicas: usize, seed_node: &'a N) -> Self {
    // TODO VNodes should be created with references to &*ring.nodes[0] instead of seed_node
    Ring {
      replicas: replicas,
      nodes: vec![Box::new(seed_node.clone())],
      vnodes: create_replicas_for_node(replicas, seed_node),
    }
  }

  pub fn contains_name<S: Into<String>>(&self, name: S) -> bool {
    let str_name = name.into();
    self.nodes.iter().any(|ref node| node.name() == str_name)
  }

  pub fn contains(&self, search_node: &'a N) -> bool {
    self.contains_name(search_node.name())
  }

  pub fn add(&mut self, node: &'a N) {
    if self.contains(node) {
      // The Ring already has this node
      return;
    }

    self.nodes.push(Box::new(node.clone()));

    let mut i = 0;
    let mut new_vnodes = create_replicas_for_node(self.replicas, node);

    // Insert our new vnodes in place.
    while i < self.vnodes.len() {
      if new_vnodes.is_empty() {
        break;
      }

      if self.vnodes[i] >= new_vnodes[0] {
        self.vnodes.insert(i, new_vnodes.remove(0));
      }

      i += 1;
    }

    // If we still have nodes left then they must have smaller hashes than the
    // nodes in self.vnodes. Lets put them before the other vnodes.
    while !new_vnodes.is_empty() {
      self.vnodes.push(new_vnodes.remove(0));
    }
  }

  pub fn remove(&mut self, node: &'a N) {
    if let Ok(i) = self.nodes.binary_search_by(|box_node| (**box_node).cmp(node)) {
      self.nodes.remove(i);
      self.vnodes.retain(|ref vnode| vnode.node.eq(node));
    }
  }

  pub fn get_with_hash<S: Into<String>>(&self, hash: S) -> Option<&N>  {
    if self.vnodes.is_empty() {
      return None;
    }

    let key_hash = hash.into();

    // Find the first vnode with a hash >= key_hash. If we don't find
    // one return the first vnode instead.
    //
    self.vnodes.iter()
               .find(|&vnode| vnode.hash >= key_hash)
               .map(|ref vnode| vnode.node)
               .or_else(|| Some(self.vnodes[0].node))
  }

  pub fn get<'b, S: Into<&'b str>>(&self, key: S) -> Option<&N>  {
    self.get_with_hash(hash_key(key))
  }
}




#[cfg(test)]
mod tests {
  use super::*;
  use std::fmt;
  use std::cmp::Ordering;

  #[derive(Debug, Clone, Eq, Ord)]
  struct TestNode {
    id: String,
  }

  impl TestNode {
    fn new(id: &str) -> TestNode {
      TestNode { id: id.to_string() }
    }
  }

  impl Node for TestNode {
    fn name(&self) -> String {
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
          write!(f, "TestNode<({})>", self.name())
      }
  }


  #[test]
  fn contains_tests() {
    let test_node1 = TestNode::new("Foo");
    let test_node2 = TestNode::new("Bar");
    let ring = Ring::new(3, &test_node1);

    assert_eq!(ring.contains(&TestNode::new("Foo")), true);
    assert_eq!(ring.contains(&test_node2), false);
  }

  #[test]
  fn contains_name_tests() {
    let test_node1 = TestNode::new("Foo");
    let ring = Ring::new(3, &test_node1);

    assert_eq!(ring.contains_name("Foo"), true);
    assert_eq!(ring.contains_name("Bar"), false);
  }

  #[test]
  fn get_nodes_test() {
    let test_node1 = TestNode::new("Foo");
    let test_node2 = TestNode::new("Bar");
    let test_node3 = TestNode::new("Baz");
    let mut ring = Ring::new(3, &test_node1);

    // Initially there is only the seed node so everything maps to it
    assert_eq!(ring.get("first"), Some(&test_node1));

    ring.add(&test_node2);
    ring.add(&test_node3);

    // The ring should now be partitioned into 9 vnodes that reference 3 nodes

    // We should always be able to get back a node by using a specific vnodes key.
    // VNode keys are of the form "{node name}_{replica number}"
    //
    for node in &[&test_node1, &test_node2, &test_node3] {
      for r in 0..3 {
        let key = format!("{}_{}", node.name(), r);
        println!("Verify replica {} of node {}: {}", r, node.name(), key);
        assert_eq!(ring.get(key.as_str()), Some(*node));
      }
    }

    // Key lookup should map to the nearest node
    // TODO: test that keys map to the correct nodes
  }

  #[test]
  fn vnodes_partition_test() {
    let test_node1 = TestNode::new("Foo");
    let test_node2 = TestNode::new("Bar");
    let mut ring = Ring::new(3, &test_node1);

    assert_eq!(ring.vnodes.len(), 3);

    ring.add(&test_node2);
    assert_eq!(ring.vnodes.len(), 6);
  }

}
