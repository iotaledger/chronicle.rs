// uses (WIP)
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, Ordering};
use crate::cluster::supervisor::Nodes;
use std::sync::{Arc, Weak};
use rand::prelude::ThreadRng;
use crate::stage::reporter::Event;
use crate::worker::Error;
use crate::stage::supervisor::Reporters;
use std::collections::HashMap;
use std::cell::RefCell;
use rand::{Rng, thread_rng};
use rand::distributions::Uniform;
use std::i64::{MIN, MAX};
// types
pub type Token = i64;
pub type Msb = u8;
pub type ShardCount = u8;
pub type VnodeTuple = (Token,Token,[u8;5],&'static str, Msb, ShardCount);
pub type VnodeWithReplicas = (Token, Token, Replicas); // VnodeWithReplicas
pub type NodeId = [u8; 5]; // four-bytes ip and last byte for shard num.
pub type DC = &'static str;
type Replicas = HashMap< DC,Vec<Replica>>;
type Replica = (NodeId,Msb,ShardCount);
type Vcell = Box<dyn Vnode>;
pub type Registry = HashMap<NodeId, Reporters>;
pub type GlobalRing = (u8 ,Registry, Vcell);
pub struct Ring {
    version: u8, // option temp
    registry: Registry,
    root: Vcell,
    uniform: Uniform<u8>,
    rng: ThreadRng,
}
use std::ptr;


static mut VERSION: u8 = 0;
static mut GLOBAL_RING: NonNull<AtomicPtr<Weak<GlobalRing>>> = ptr::NonNull::dangling();

thread_local!{
    static RING: RefCell<Ring> = {
        let rng = thread_rng();
        let uniform: Uniform<u8> = Uniform::new(0,1); // move this to global const
        let registry: Registry = HashMap::new();
        let root: Vcell = Ring::initial_ring();
        // create useless weak pointer
        let version = 0;
        RefCell::new(Ring{version, registry ,root, uniform, rng})
    };
}

impl Ring {
    pub fn send(data_center: DC,replica_index: usize,token: Token,request: Event) {
        RING.with(|local| {
            local.borrow_mut().sending(data_center, replica_index, token, request)
        }
        )
    }
    fn sending(&mut self,data_center: DC,replica_index: usize,token: Token,request: Event) {
        unsafe {
            if  VERSION != self.version {
                // load weak and upgrade to arc if strong_count > 0;
                if let Some(mut arc) = Weak::upgrade(&*GLOBAL_RING.as_mut().load(Ordering::Relaxed)) {
                    let (version, registry, root) = Arc::make_mut(&mut arc);
                    // update the local ring
                    self.version = version.clone();
                    self.registry = registry.clone();
                    self.root = root.clone();
                };
            }
        }
        // send request.
        self.root.as_mut()
        .search(token)
        .send(data_center, replica_index, token, request, &mut self.registry, &mut self.rng, self.uniform);
    }
    fn initial_ring() -> Vcell {
        DeadEnd::initial_vnode()
    }
}
trait SmartId {
    fn send_reporter(&mut self, token: Token, registry: &mut Registry, rng: &mut ThreadRng, uniform: Uniform<u8>, request: Event) ;
}
impl SmartId for Replica {
    fn send_reporter(&mut self, token: Token, registry: &mut Registry, rng: &mut ThreadRng, uniform: Uniform<u8>, request: Event) {
        // shard awareness algo,
        self.0[4] = (((((token as i128 + MIN as i128) as u64)
         << self.1) as u128 * self.2 as u128) >> 64) as u8;
        registry.get_mut(&self.0).unwrap()
        .get_mut(&rng.sample(uniform)).unwrap()
        .send(request);
    }
}

pub trait Endpoints: EndpointsClone {
    fn send(&mut self,data_center: DC, replica_index: usize, token: Token, request: Event, registry: &mut Registry, rng: &mut ThreadRng, uniform: Uniform<u8>);
}

pub trait EndpointsClone {
    fn clone_box(&self) -> Box<dyn Endpoints>;
}

impl<T> EndpointsClone for T
where
    T: 'static + Endpoints + Clone,
{
    fn clone_box(&self) -> Box<dyn Endpoints> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Endpoints> {
    fn clone(&self) -> Box<dyn Endpoints> {
        self.clone_box()
    }
}


impl Endpoints for Replicas {
    fn send(&mut self,data_center: DC, replica_index: usize, token: Token, request: Event, mut registry: &mut Registry, mut rng: &mut ThreadRng, uniform: Uniform<u8>) {
        self.get_mut(&data_center).unwrap()[replica_index].send_reporter(token, &mut registry, &mut rng, uniform, request);
    }
}
impl Endpoints for Option<Replicas> {
    // this method will be invoked when we store Replicas as None.
    // used for initial ring to simulate the reporter and respond to worker(self) with NoRing error
    fn send(&mut self,_: DC, _: usize,_: Token, request: Event, _: &mut Registry, _: &mut ThreadRng, _uniform: Uniform<u8>) {
        // simulate reporter behivour,
        if let Event::Request{mut worker, payload: _} = request{
            worker.send_error(Error::NoRing);
        };
    }
}

pub trait Vnode: VnodeClone {
    fn search(&mut self, token: Token) -> &mut Box<dyn Endpoints> ;

}
pub trait VnodeClone {
    fn clone_box(&self) -> Box<dyn Vnode>;
}

impl<T> VnodeClone for T
where
    T: 'static + Vnode + Clone,
{
    fn clone_box(&self) -> Box<dyn Vnode> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Vnode> {
    fn clone(&self) -> Box<dyn Vnode> {
        self.clone_box()
    }
}

impl Vnode for Mild {
    fn search(&mut self,token: Token) -> &mut Box<dyn Endpoints> {
        if token > self.left && token <= self.right {
            &mut self.replicas
        } else if token <= self.left {
            // proceed binary search; shift left.
            self.left_child.search(token)
        } else {
            // proceed binary search; shift right
            self.right_child.search(token)
        }
    }
}

impl Vnode for LeftMild {
    fn search(&mut self,token: Token) -> &mut Box<dyn Endpoints> {
        if token > self.left && token <= self.right {
            &mut self.replicas
        } else {
            // proceed binary search; shift left
            self.left_child.search(token)
        }
    }
}

impl Vnode for DeadEnd {
    fn search(&mut self,_token: Token) -> &mut Box<dyn Endpoints> {
        &mut self.replicas
    }
}

// this struct represent a vnode without left or right child,
// we don't need to set conditions because it's a deadend during search(),
// and condition must be true.
#[derive(Clone)]
struct DeadEnd {
    replicas: Box<dyn Endpoints>,
}

impl DeadEnd {
    fn initial_vnode() -> Vcell {
        Box::new(DeadEnd{replicas: Box::new(None)})
    }
}
// this struct represent the mild possible vnode(..)
// condition: token > left, and token <= right
#[derive(Clone)]
struct Mild {
    left: Token,
    right: Token,
    left_child: Vcell,
    right_child: Vcell,
    replicas: Box<dyn Endpoints>,
}

// as mild but with left child.
#[derive(Clone)]
struct LeftMild {
    left: Token,
    right: Token,
    left_child: Vcell,
    replicas: Box<dyn Endpoints>,
}

// Ring Builder Work in progress
fn compute_vnode(chain: &[(Token, Token, Replicas)]) -> Vcell {
    let index = chain.len()/2;
    let (left, right) = chain.split_at(index);
    let (vnode, right) = right.split_first().unwrap();
    if right.is_empty() && left.is_empty() {
        // then the parent_vnode without any child so consider it deadend
        Box::new(DeadEnd{replicas: Box::new(vnode.2.to_owned())})
    } else if !right.is_empty() && !left.is_empty() {
        // parent_vnode is mild with left /right childern
        // compute both left and right
        let left_child = compute_vnode(left);
        let right_child = compute_vnode(right);
        Box::new(
            Mild{left: vnode.0,
                right: vnode.1,
                left_child: left_child,
                right_child: right_child,
                replicas: Box::new(vnode.2.to_owned())}
            )
    } else { // if !left.is_empty() && right.is_empty()
        // parent_vnode is leftmild
        Box::new(
            LeftMild{
                left:vnode.0,
                right:vnode.1,
                left_child: compute_vnode(left),
                replicas: Box::new(vnode.2.to_owned())}
            )
    }
}

fn walk_clockwise(starting_index: usize, end_index: usize,vnodes: &Vec<VnodeTuple>,replicas: &mut Replicas) {
    for i in starting_index..end_index {
        // fetch replica
        let (_, _, node_id, dc, msb, shard_count) = vnodes[i];
        let replica: Replica = (node_id, msb, shard_count);
        // now push it to Replicas
        match replicas.get_mut(dc) {
            Some(vec_replicas_in_dc) => {
                if !vec_replicas_in_dc.contains(&replica) {
                    vec_replicas_in_dc.push(replica)
                }
            }
            None => {
                let vec_replicas_in_dc = vec![replica];
                replicas.insert(dc, vec_replicas_in_dc);
            }
        }
    }
}



#[test]
fn generate_and_compute_fake_ring() {
    let mut rng = thread_rng();
    let uniform = Uniform::new(MIN, MAX);
    // create test token_range vector // the token range should be fetched from scylla node.
    let mut tokens: Vec<(Token, NodeId, DC)> = Vec::new();
    // 4 us nodes ids
    let us_node_id_1: NodeId = [127,0,0,1,0];
    let us_node_id_2: NodeId = [127,0,0,2,0];
    let us_node_id_3: NodeId = [127,0,0,3,0];
    let us_node_id_4: NodeId = [127,0,0,4,0];
    // 3 eu nodes ids
    let eu_node_id_1: NodeId = [128,0,0,1,0];
    let eu_node_id_2: NodeId = [128,0,0,2,0];
    let eu_node_id_3: NodeId = [128,0,0,3,0];
    for _ in 0..256 {
        // 4 nodes in US Datacenter
        tokens.push((rng.sample(uniform), us_node_id_1, "US"));
        tokens.push((rng.sample(uniform), us_node_id_2, "US"));
        tokens.push((rng.sample(uniform), us_node_id_3, "US"));
        tokens.push((rng.sample(uniform), us_node_id_4, "US"));
        // 3 nodes in EU Datacenter
        tokens.push((rng.sample(uniform), eu_node_id_1, "EU"));
        tokens.push((rng.sample(uniform), eu_node_id_2, "EU"));
        tokens.push((rng.sample(uniform), eu_node_id_3, "EU"));
    }
    // sort tokens by token
    tokens.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    // compute replicas for each vnode
    let mut vnodes = Vec::new();
    let mut recent_left = MIN;
    for (right, node_id, dc) in &tokens {
        // create vnode(starting from min)
        let vnode = (recent_left,*right, *node_id, dc.clone(), 12, 8); // fake msb/shardcount
        // push to vnodes
        vnodes.push(vnode);
        // update recent_left to right
        recent_left = *right;
    }
    // we don't forget to add max vnode to our token range
    let recent_node_id = tokens.last().unwrap().1;
    let recent_dc = tokens.last().unwrap().2;
    let max_vnode = (recent_left, MAX, recent_node_id, recent_dc, 12, 8); //
    vnodes.push(max_vnode);
    // compute all possible replicas in advance for each vnode in vnodes
    // prepare ring chain
    let mut chain = Vec::new();
    let mut starting_index = 0;
    for (left, right, _, _, _, _) in &vnodes {
        let mut replicas: Replicas = HashMap::new();
        // first walk clockwise phase (start..end)
        walk_clockwise(starting_index, vnodes.len(), &vnodes, &mut replicas);
        // second walk clockwise phase (0..start)
        walk_clockwise(0, starting_index, &vnodes, &mut replicas);
        // update starting_index
        starting_index += 1;
        // create vnode
        chain.push((*left, *right, replicas));
    }
    // build computed binary search tree from chain
    // we start spliting from the root which is chain.len()/2
    // for example if chain length is 3 then the root vnode is at 3/2 = 1
    // and it will be mild where both of its childern are deadends.
    let _root = compute_vnode(&chain);
}

pub fn build_ring(nodes: &Nodes, registry: Registry, version: u8) -> Arc<GlobalRing> {
    let mut tokens = Vec::new(); // complete tokens-range
    // iter nodes
    for (_, node_info) in nodes {
        // we generate the tokens li
        for t in &node_info.tokens  {
            tokens.push(t)
        }
    }
    // sort_unstable_by token
    tokens.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    // create vnodes tuple from tokens
    let mut vnodes = Vec::new();
    let mut recent_left = MIN;
    for (right, node_id, dc, msb, shard_count) in &tokens {
        // create vnode tuple (starting from min)
        let vnode = (recent_left,*right, *node_id, *dc, *msb, *shard_count);
        // push to vnodes
        vnodes.push(vnode);
        // update recent_left to right
        recent_left = *right;
    }
    // the check bellow is only to make sure if scylla-node didn't already
    // randmoly didn't gen the MIN token by luck.
    // confirm if the vnode_min is not already exist in our token range
    if vnodes.first().unwrap().1 == MIN { //
        // remove it, otherwise the first vnode will be(MIN, MIN, ..) and invalidate vnode conditions
        vnodes.remove(0);
    };
    // we don't forget to add max vnode to our token range only if not already presented,
    // the check bellow is only to make sure if scylla-node didn't already
    // randmoly gen the MAX token by luck.
    // the MAX to our last vnode(the largest token )
    let last_vnode = vnodes.last().unwrap();
    // confirm if the vnode max is not present in our token-range
    if last_vnode.1 != MAX {
        let max_vnode = (recent_left, MAX, last_vnode.2, last_vnode.3,last_vnode.4, last_vnode.5);
        // now push it
        vnodes.push(max_vnode);
    }
    // compute_ring
    let root_vnode = compute_ring(&vnodes);
    // create arc_ring
    let arc_ring = Arc::new((version ,registry, root_vnode));
    // update the global ring
    unsafe {
        // swap will take the ownership to drop the old weak
        GLOBAL_RING.as_mut().swap(&mut Arc::downgrade(&arc_ring), Ordering::Relaxed);
        // update version with new one.// this must be atomic and safe because it's u8.
        VERSION = version;
    }
    // return new arc_ring
    arc_ring
}

fn compute_ring(vnodes: &Vec<VnodeTuple>) -> Vcell {
    // compute chain (vnodes with replicas)
    let chain = compute_chain(vnodes);
    // compute balanced binary tree
    compute_vnode(&chain)
}

fn compute_chain(vnodes: &Vec<VnodeTuple>) -> Vec<(Token, Token, Replicas)> {
    // compute all possible replicas in advance for each vnode in vnodes
    // prepare ring chain
    let mut chain = Vec::new();
    let mut starting_index = 0;
    for (left, right, _, _, _, _) in vnodes {
        let mut replicas: Replicas = HashMap::new();
        // first walk clockwise phase (start..end)
        walk_clockwise(starting_index, vnodes.len(), &vnodes, &mut replicas);
        // second walk clockwise phase (0..start)
        walk_clockwise(0, starting_index, &vnodes, &mut replicas);
        // update starting_index
        starting_index += 1;
        // create vnode
        chain.push((*left, *right, replicas));
    }
    chain
}
