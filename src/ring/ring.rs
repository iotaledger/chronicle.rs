// uses
use crate::stage::reporter::Event;
use std::collections::HashMap;
use std::cell::RefCell;
use std::i64::{MIN, MAX};
// types
type Token = i64;
type Msb = u8;
type ShardsNum = u8;
type NodeId = u16;
type DC = String;
type Replicas = HashMap< DC,Vec<(NodeId,Msb,ShardsNum)> >;
type Vcell = Box<dyn Vnode>;
type RingVector =  Vec<Vcell> ;

// thread local ring
thread_local!{
    static RING: RefCell<Vec<Vcell>> = RefCell::new(Vec::new());
}

// trait
trait Vnode {
    fn search(&self, ring_vector: &RingVector,data_center: DC,replica_index: u8, token: Token, request: Event);
    fn next(&self,index: usize, ring_vector: &RingVector,data_center: DC,replica_index: u8, token: Token, request: Event) {
        ring_vector[index].search(ring_vector,data_center,replica_index, token, request);
    }
}
trait Ring {
    fn send(&self,data_center: DC,replica_index: u8,  token: Token, request: Event) ;
}
impl Ring for RingVector {
    fn send(&self,data_center: DC, replica_index: u8, token: Token, request: Event) {
        // we do binary search from center of the ringvector
        self[self.len()/2].search(self,data_center,replica_index, token, request)
    }
}

trait Endpoints {
    fn send(&self,data_center: DC, replica_index: u8, token: Token, request: Event)  {
        unimplemented!()
    }
}
impl Endpoints for Replicas {
    fn send(&self,data_center: DC, replica_index: u8, token: Token, request: Event) {
        unimplemented!()
    }
}

impl Vnode for Low {
    fn search(&self, _ring_vector: &RingVector,data_center: DC,replica_index: u8, token: Token, request: Event) {
        if token >= MIN && token <= self.right { // must be true TODO remove if condition
            self.replicas.send(data_center, replica_index, token, request);
        }
    }
}

impl Vnode for Mild {
    fn search(&self, ring_vector: &RingVector,data_center: DC,replica_index: u8, token: Token, request: Event) {
        if token > self.left && token <= self.right {
            self.replicas.send(data_center, replica_index, token, request);
        } else if token <= self.left {
            // proceed binary search; shift left
            self.next(self.left_index, ring_vector,data_center,replica_index, token, request);
        } else if token > self.right {
            // proceed binary search; shift right
            self.next(self.right_index, ring_vector,data_center,replica_index, token, request);
        }
    }
}

impl Vnode for High {
    fn search(&self, ring_vector: &RingVector,data_center: DC,replica_index: u8, token: Token, request: Event) {
        // todo reorder if conditions
        if token > self.left && token <= MAX {
            self.replicas.send(data_center, replica_index, token, request);
        } else if token <= self.left {
            // proceed binary search; shift left
            self.next(self.left_index, ring_vector,data_center,replica_index, token, request);
        }
    }
}

impl Vnode for All {
    fn search(&self,ring_vector: &RingVector,data_center: DC,replica_index: u8, token: Token, request: Event) {
        self.replicas.send(data_center, replica_index, token, request);
    }
}

// this struct represent the lowest possible vnode(min..)
// condition: token >= MIN, and token <= right
struct Low {
    index: usize,
    right: Token,
    right_index: usize,
    replicas: Replicas,
}

// this struct represent the mild possible vnode(..)
// condition: token > left, and token <= right
struct Mild {
    index: usize,
    left: Token,
    right: Token,
    left_index: usize,
    right_index: usize,
    replicas: Replicas,
}

// this struct represent the highest possible vnode(..max)
// condition: token > left, and token <= MAX
struct High {
    index: usize,
    left: Token,
    left_index: usize,
    replicas: Replicas,
}

// this struct represent all possible vnode(min..max)
// condition: token >= MIN, and token <= right = MAX
struct All {
    index: usize,
    replicas: Replicas,
}
