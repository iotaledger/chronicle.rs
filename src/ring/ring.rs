// uses (WIP)
use rand::prelude::ThreadRng;
use crate::stage::reporter::Event;
use crate::stage::supervisor::Reporters;
use std::collections::HashMap;
use std::cell::RefCell;
use rand::{Rng, thread_rng};
use rand::distributions::Uniform;
use std::i64::{MIN, MAX};
// types
type Token = i64;
type Msb = u8;
type ShardsNum = u8;
type NodeId = [u8; 5]; // four-bytes ip and last byte for shard num.
type DC = String;
type Replicas = HashMap< DC,Vec<Replica>>;
type Replica = (NodeId,Msb,ShardsNum);
type Vcell = Box<dyn Vnode>;
type RingVector =  Vec<Vcell>;
type Registry = HashMap<NodeId, Reporters>;

struct Ring {
    registry: Registry,
    root: Option<Box<dyn Vnode>>,
    uniform: Uniform<u8>,
    rng: ThreadRng,
}

thread_local!{
    static RING: RefCell<Ring> = {
        let rng = rand::thread_rng();
        let uniform: Uniform<u8> = Uniform::new(0,1);
        let registry: Registry = HashMap::new();
        let root: Option<Box<dyn Vnode>> = None;
        RefCell::new(Ring{registry ,root, uniform, rng})
    };
}

impl Ring {
    fn send(data_center: DC,replica_index: usize,token: Token,request: Event) {
        RING.with(|local| {
            local.borrow_mut().sending(data_center, replica_index, token, request)
        }
        )
    }
    fn sending(&mut self,data_center: DC,replica_index: usize,token: Token,request: Event) {
        self.root.as_mut().unwrap()
        .search(token)
        .send(data_center, replica_index, token, request, &mut self.registry, &mut self.rng, self.uniform)
    }
}
trait SmartId {
    fn send_reporter(&mut self, token: Token, registry: &mut Registry, rng: &mut ThreadRng, uniform: Uniform<u8>, request: Event) ;
}
impl SmartId for Replica {
    fn send_reporter(&mut self, token: Token, registry: &mut Registry, rng: &mut ThreadRng, uniform: Uniform<u8>, request: Event) {
        // shard awareness algo, (todo: to be tested)
        self.0[4] = ( (( ((token - MIN) << self.1) as u128 )* (self.2 as u128)) >> 64 ) as u8;
        registry.get_mut(&self.0).unwrap()
        .get_mut(&rng.sample(uniform)).unwrap()
        .send(request);
    }
}

trait Endpoints {
    fn send(&mut self,data_center: DC, replica_index: usize, token: Token, request: Event, registry: &mut Registry, rng: &mut ThreadRng, uniform: Uniform<u8>);
}
impl Endpoints for Replicas {
    fn send(&mut self,data_center: DC, replica_index: usize, token: Token, request: Event, mut registry: &mut Registry, mut rng: &mut ThreadRng, uniform: Uniform<u8>) {
        self.get_mut(&data_center).unwrap()[replica_index].send_reporter(token, &mut registry, &mut rng, uniform, request);
    }
}


trait Vnode {
    fn search(&mut self, token: Token) -> &mut Replicas ;
}

impl Vnode for Low {
    fn search(&mut self, token: Token) -> &mut Replicas  {
        &mut self.replicas
    }

}


impl Vnode for Mild {
    fn search(&mut self,token: Token) -> &mut Replicas {
        if token > self.left && token <= self.right {
            &mut self.replicas
        } else if token <= self.left {
            // proceed binary search; shift left .
            self.left_child.search(token)
        } else {
            // proceed binary search; shift right
            self.right_child.search(token)
        }
    }
}

impl Vnode for High {
    fn search(&mut self,token: Token) -> &mut Replicas {
        if token > self.left && token <= MAX {
            &mut self.replicas
        } else {
            // proceed binary search; shift left
            self.left_child.search(token)
        }
    }
}

impl Vnode for All {
    fn search(&mut self,token: Token) -> &mut Replicas {
        &mut self.replicas
    }
}

// this struct represent the lowest possible vnode(min..)
// condition: token >= MIN, and token <= right
struct Low {
    right: Token,
    right_child: Box<dyn Vnode>,
    replicas: Replicas,
}

// this struct represent the mild possible vnode(..)
// condition: token > left, and token <= right
struct Mild {
    left: Token,
    right: Token,
    left_child: Box<dyn Vnode>,
    right_child: Box<dyn Vnode>,
    replicas: Replicas,
}

// this struct represent the highest possible vnode(..max)
// condition: token > left, and token <= MAX
struct High {
    left: Token,
    left_child: Box<dyn Vnode>,
    replicas: Replicas,
}

// this struct represent all possible vnode(min..max)
// condition: token >= MIN, and token <= right = MAX
struct All {
    replicas: Replicas,
}
