// uses (WIP)
use std::sync::Arc;
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
type DC = &'static str;
type Replicas = HashMap< DC,Vec<Replica>>;
type Replica = (NodeId,Msb,ShardsNum);
type Vcell = Box<dyn Vnode>;
type Registry = HashMap<NodeId, Reporters>;
type Root = Option<Vcell>; // todo remove option
struct Ring {
    arc: Option<*const (Registry, Root)>, // option temp
    registry: Registry,
    root: Root,
    uniform: Uniform<u8>,
    rng: ThreadRng,
}

static mut ARC_RING: Option<*const (Registry, Root)> = None; // to be moved to cluster

thread_local!{
    static RING: RefCell<Ring> = {
        let arc = None; // arc will help the cluster to know if the ring propagated to all threads
        let rng = rand::thread_rng();
        let uniform: Uniform<u8> = Uniform::new(0,1);
        let registry: Registry = HashMap::new();
        let root: Option<Vcell> = None;
        RefCell::new(Ring{arc, registry ,root, uniform, rng})
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
        // check if global ARC_RING not up to date.. TODO confirm contract design here.
        unsafe {
            if ARC_RING != self.arc {
                // access the global raw and clone it to increase the strong_count
                let mut arc = Arc::from_raw(ARC_RING.unwrap()).clone();
                // clone the new registry and root
                let (registry, root) = Arc::make_mut(&mut arc);
                // update self registry and drop the old one
                self.registry = registry.clone();
                // update self root and drop the old one
                self.root = root.clone();
                // consume the old arc as it's stored in *const raw format
                if let Some(old_raw) = self.arc {
                    // convert it back to arc to prevent memory leak and
                    Arc::from_raw(old_raw);
                }; // at this line old_raw_arc should be drop it
                // update self arc by invoking into_raw to consume it
                self.arc = Some(Arc::into_raw(arc)); // now arc supposed to point to same location as ARC_RING, and strong_count should had increased by one.
            }
        }
        // send request.
        self.root.as_mut().unwrap()
        .search(token)
        .send(data_center, replica_index, token, request, &mut self.registry, &mut self.rng, self.uniform);
    }
}
trait SmartId {
    fn send_reporter(&mut self, token: Token, registry: &mut Registry, rng: &mut ThreadRng, uniform: Uniform<u8>, request: Event) ;
}
impl SmartId for Replica {
    fn send_reporter(&mut self, token: Token, registry: &mut Registry, rng: &mut ThreadRng, uniform: Uniform<u8>, request: Event) {
        // shard awareness algo, (todo: to be tested)
        self.0[4] = (((((token as i128 + MIN as i128) as u64)
         << self.1) as u128 * self.2 as u128) >> 64) as u8;
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

impl Clone for Vcell {
    fn clone(&self) -> Self { self.clone() }
}

trait Vnode {
    fn search(&mut self, token: Token) -> &mut Replicas ;
}

impl Vnode for Mild {
    fn search(&mut self,token: Token) -> &mut Replicas {
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
    fn search(&mut self,token: Token) -> &mut Replicas {
        if token > self.left && token <= self.right {
            &mut self.replicas
        } else {
            // proceed binary search; shift left
            self.left_child.search(token)
        }
    }
}

impl Vnode for DeadEnd {
    fn search(&mut self,_token: Token) -> &mut Replicas {
        &mut self.replicas
    }
}

// this struct represent a vnode without left or right child,
// we don't need to set conditions because it's a deadend during search(),
// and condition must be true.
struct DeadEnd {
    replicas: Replicas,
}

// this struct represent the mild possible vnode(..)
// condition: token > left, and token <= right
struct Mild {
    left: Token,
    right: Token,
    left_child: Vcell,
    right_child: Vcell,
    replicas: Replicas,
}

// as mild but with left child.
struct LeftMild {
    left: Token,
    right: Token,
    left_child: Vcell,
    replicas: Replicas,
}

// Ring Builder Work in progress
fn compute_vnode(chain: &[(Token, Token, Replicas)]) -> Vcell {
    let index = chain.len()/2;
    let (left, right) = chain.split_at(index);
    let (vnode, right) = right.split_first().unwrap();
    if right.is_empty() && left.is_empty() {
        // then the parent_vnode without any child so consider it deadend
        Box::new(DeadEnd{replicas: vnode.2.to_owned()})
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
                replicas: vnode.2.to_owned()}
            )
    } else { // if !left.is_empty() && right.is_empty()
        // parent_vnode is leftmild
        Box::new(
            LeftMild{
                left:vnode.0,
                right:vnode.1,
                left_child: compute_vnode(left),
                replicas: vnode.2.to_owned()}
            )
    }
}

fn walk_clockwise(starting_index: usize, end_index: usize,vnodes: &Vec<(Token,Token,[u8;5],&'static str)>,replicas: &mut Replicas) {
    for i in starting_index..end_index {
        // fetch replica
        let (_, _, node_id, dc) = vnodes[i];
        let replica: Replica = (node_id, 12, 8); // use fake msb shardsnum for now.
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
        let vnode = (recent_left,*right, *node_id, dc.clone());
        // push to vnodes
        vnodes.push(vnode);
        // update recent_left to right
        recent_left = *right;
    }
    // we don't forget to add max vnode to our token range
    let recent_node_id = tokens.last().unwrap().1;
    let recent_dc = tokens.last().unwrap().2;
    let max_vnode = (recent_left, MAX, recent_node_id, recent_dc);
    vnodes.push(max_vnode);
    // compute all possible replicas in advance for each vnode in vnodes
    // prepare ring chain
    let mut chain = Vec::new();
    let mut starting_index = 0;
    for (left, right, _, _) in &vnodes {
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
