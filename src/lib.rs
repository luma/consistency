#![feature(plugin)]
#![plugin(clippy)]
#![feature(custom_derive)]
#[macro_use]
extern crate crypto;

use std::vec::Vec;
use std::boxed::Box;
use std::cmp::Ordering;
use std::fmt::Display;
use std::fmt::Debug;

use crypto::digest::Digest;
use crypto::sha1::Sha1;

/// Generate a Sha1 from a string key.
///
fn hash_of(key: &str) -> String {
  let mut hasher = Sha1::new();
  hasher.input_str(key);
  hasher.result_str()
}

// TODO Should be hashable as well
pub trait Node: Clone + Debug + Display + Eq + Ord {
  fn name(&self) -> String;
}

#[derive(Debug, Eq)]
pub struct VNode <'a, N> where N: Node + 'a {
  replica: usize,
  node: &'a N,
  hash: String,
}

impl<'a, N: Node + 'a> VNode<'a, N> {
  pub fn new(replica: usize, node: &'a N) -> Self {
    let hash = hash_of(format!("{}_{}", node.name(), replica).as_str());

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

impl<'a, N: Node + 'a> PartialEq<String> for VNode<'a, N> {
  fn eq(&self, other: &String) -> bool {
    self.hash == *other
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

// TODO implement Cmp, etc for VNode

pub type VNodes<'a, N> = Vec<VNode<'a, N>>;

pub struct Ring <'a, N> where N: Node + 'a {
  pub replicas: usize,
  pub nodes: Vec<Box<N>>,
  pub vnodes: VNodes<'a, N>,
}

impl<'a, N: Node + 'a> Ring<'a, N> {
  pub fn new(replicas: usize, seed_node: &'a N) -> Self {
    // TODO VNodes should be created with references to &*ring.nodes[0] instead of seed_node
    let mut vnodes = (0..replicas).map(|r| VNode::new(r, seed_node))
                              .collect::<VNodes<'a, N>>();
    vnodes.sort();

    Ring {
      replicas: replicas,
      nodes: vec![Box::new(seed_node.clone())],
      vnodes: vnodes,
    }
  }

  pub fn contains(&self, search_node: &'a N) -> bool {
    let search_name = search_node.name();
    self.nodes.iter().any(|node| node.name() == search_name)
  }

  pub fn add_node(&mut self, node: &'a N, replicas: usize) {
    if self.contains(node) {
      // The Ring already has this node
      return;
    }

    self.nodes.push(Box::new(node.clone()));

    for r in 0..replicas {
      self.vnodes.push(VNode::new(r, &node))
    }

    // TODO This sort is stable, but it still allocates 2 * self.vnodes.len(). We should
    // just insert the new vnodes exactly into place:
    //   1. sort the new nodes
    //   2. walk self.vnodes once inserting the first new node when it's hash is smaller or equal to the existing node
    //   3. if new nodes isn't empty after then add them to the front on self.vnodes
    // NOTE This assumes that Cmp is implemented for VNode + VNode
    self.vnodes.sort();
  }

  pub fn remove_node(&mut self, node: N) {
    if let Ok(i) = self.nodes.binary_search_by(|box_node| (**box_node).cmp(&node)) {
      self.nodes.remove(i);
      self.vnodes.retain(|vnode| vnode.node.eq(&node));
    }
  }

  pub fn lookup(&self, key: &str) -> Option<&N>  {
    if self.vnodes.is_empty() {
      return None;
    }

    let key_hash = hash_of(key);

    // Find the first vnode with a hash >= key_hash. If we don't find
    // one return the first vnode instead.
    //
    self.vnodes.iter()
               .find(|&vnode| vnode.hash >= key_hash)
               .map(|vnode| vnode.node)
               .or_else(|| Some(self.vnodes[0].node))
  }
}




#[cfg(test)]
mod tests {
  use super::*;
  use std::fmt;

  #[derive(Debug, Clone)]
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

  impl fmt::Display for TestNode {
      fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
          write!(f, "TestNode<({})>", self.name())
      }
  }


  #[test]
  fn contains_tests() {
    let test_node1 = TestNode::new("Foo");
    let test_node2 = TestNode::new("Bar");
    let ring = Ring::new(8, &test_node1);

    assert_eq!(ring.contains(TestNode::new("Foo")), true);
    assert_eq!(ring.contains(test_node2), false);
  }

  #[test]
  fn contains_name_tests() {
    let test_node1 = TestNode::new("Foo");
    let ring = Ring::new(8, &test_node1);

    assert_eq!(ring.contains_name("Foo"), true);
    assert_eq!(ring.contains_name("Bar"), false);
  }

  #[test]
  fn size_test() {
    let test_node1 = TestNode::new("Foo");
    let ring = Ring::new(8, &test_node1);
    println!("VNODES {}", ring.vnodes.len());
    assert_eq!(ring.vnodes.len(), 8);
  }

}
