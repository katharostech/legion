use crate::ComponentTypeId;
use hibitset::{BitSet, BitSetLike};
use std::collections::{HashMap, HashSet};

trait Accessor {
    fn reads(&self) -> &[ComponentTypeId];
    fn writes(&self) -> &[ComponentTypeId];
}

trait Barrier: Ord + std::hash::Hash + Clone + std::fmt::Debug {}

trait JobDescriptor: std::fmt::Debug {
    type Accessor: Accessor;
    type Barrier: Barrier;
    fn accessor(&self) -> &Self::Accessor;
    fn run_after(&self) -> Option<Self::Barrier>;
    fn finish_before(&self) -> Option<Self::Barrier>;
}
#[derive(Debug)]
enum Node<'a, J: JobDescriptor> {
    Job(&'a J),
    Barrier(J::Barrier),
    Root,
}

#[derive(Debug)]
struct DispatchState<'a, J: JobDescriptor> {
    sorted_jobs: Vec<&'a Node<'a, J>>,
    jobs_completed: BitSet,
    jobs_scheduled: BitSet,
    job_deps: Vec<BitSet>,
}
enum ScheduleResult<'a, J: JobDescriptor> {
    Schedule(&'a Node<'a, J>, usize),
    WaitingForJob,
    Done,
}
impl<'a, J: JobDescriptor> DispatchState<'a, J> {
    pub fn next_job(&mut self) -> ScheduleResult<'a, J> {
        println!("scheduling with completed {:#?}", self.jobs_completed);
        let mut waiting = false;
        for i in 0..self.sorted_jobs.len() {
            if self.jobs_scheduled.contains(i as u32) == false {
                waiting = true;
                let deps = &self.job_deps[i];
                // first AND between deps and jobs_completed to retain only the relevant bits,
                // then XOR between deps and the result to check if there's a difference

                if (deps ^ (deps & &self.jobs_completed))
                    .iter()
                    .next()
                    .is_none()
                // .is_empty() is buggy for BitSetXOR/BitSetAnd
                {
                    self.jobs_scheduled.add(i as u32);
                    return ScheduleResult::Schedule(self.sorted_jobs[i], i);
                }
            }
        }
        if waiting {
            ScheduleResult::WaitingForJob
        } else {
            ScheduleResult::Done
        }
    }

    pub fn complete_job(&mut self, job_idx: usize) {
        self.jobs_completed.add(job_idx as u32);
    }

    pub fn reset(&mut self) {
        self.jobs_completed.clear();
        self.jobs_scheduled.clear();
    }
}

type JobGraph<'a, J> = petgraph::graph::Graph<Node<'a, J>, ()>;

fn build_dispatch_state<'a, T: JobDescriptor>(graph: &'a JobGraph<'a, T>) -> DispatchState<'a, T> {
    use petgraph::visit::EdgeRef;
    // topologically sort graph to optimize iteration for unscheduled jobs
    let mut sorted_nodes =
        petgraph::algo::toposort(&graph, None).expect("failed to sort job graph");
    sorted_nodes.reverse();
    // extract a bitset for each node that defines their dependencies in terms of indices into sorted_nodes
    let job_deps = sorted_nodes
        .iter()
        .map(|n| {
            let dep_indices = graph
                .edges_directed(*n, petgraph::Direction::Outgoing)
                .filter_map(|e| sorted_nodes.iter().position(|n| *n == e.target()));
            let mut bitset = BitSet::new();
            for idx in dep_indices {
                bitset.add(idx as u32);
            }
            bitset
        })
        .collect();
    let sorted_jobs: Vec<_> = sorted_nodes.into_iter().map(|n| &graph[n]).collect();
    DispatchState {
        jobs_completed: BitSet::with_capacity(sorted_jobs.len() as u32),
        jobs_scheduled: BitSet::with_capacity(sorted_jobs.len() as u32),
        sorted_jobs,
        job_deps,
    }
}

fn generate_job_graph<'a, T: JobDescriptor>(jobs: &'a [T]) -> JobGraph<'a, T> {
    // ensure job barrier relationships make sense
    for j in jobs {
        if let Some(a) = j.run_after() {
            if let Some(b) = j.finish_before() {
                assert!(
                    a < b,
                    "Invalid job ordering: finish_before is before run_after for job {:?}",
                    j
                );
            }
        }
    }
    // find all used barriers and sort them
    let mut barriers = HashSet::new();
    for j in jobs {
        if let Some(b) = j.run_after() {
            barriers.insert(b);
        }
        if let Some(b) = j.finish_before() {
            barriers.insert(b);
        }
    }
    let mut barriers: Vec<T::Barrier> = barriers.into_iter().collect();
    barriers.sort();

    // sort jobs by barrier order using a stable sort to retain registration order
    let mut sorted_jobs: Vec<&T> = jobs.iter().collect();
    sorted_jobs.sort_by(|x, y| {
        use std::cmp::Ordering;
        let x_first = x.run_after().or_else(|| x.finish_before());
        let y_first = y.run_after().or_else(|| y.finish_before());
        if x_first.is_none() && y_first.is_some() {
            Ordering::Less
        } else if x_first.is_some() && y_first.is_none() {
            Ordering::Greater
        } else if x_first.is_none() && y_first.is_none() {
            Ordering::Equal
        } else {
            x_first.unwrap().cmp(&y_first.unwrap())
        }
    });

    let mut g = JobGraph::<T>::new();
    let root_node = g.add_node(Node::Root);
    // Create nodes for barriers and connect them
    let mut barrier_nodes = HashMap::new();
    barrier_nodes.insert(None, root_node);
    let mut prev_node = root_node;
    for b in barriers {
        let node = g.add_node(Node::Barrier(b.clone()));
        barrier_nodes.insert(Some(b), node);
        g.add_edge(node, prev_node, ());
        prev_node = node;
    }
    // Create nodes for jobs and create edges for resource modifications
    let mut last_mutated: HashMap<ComponentTypeId, petgraph::graph::NodeIndex> = HashMap::new();
    for j in sorted_jobs {
        let job_node = g.add_node(Node::Job(j));
        g.add_edge(job_node, barrier_nodes[&j.run_after()], ());
        if j.finish_before().is_some() {
            g.add_edge(barrier_nodes[&j.finish_before()], job_node, ());
        }
        let accessor = j.accessor();
        for read in accessor.reads() {
            if let Some(n) = last_mutated.get(read) {
                g.add_edge(job_node, *n, ());
            }
        }
        for write in accessor.writes() {
            if let Some(n) = last_mutated.get(write) {
                g.add_edge(job_node, *n, ());
            }
            last_mutated.insert(*write, job_node);
        }
    }
    g
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Debug, Clone)]
    struct TestAccessor {
        reads: Vec<ComponentTypeId>,
        writes: Vec<ComponentTypeId>,
    }
    impl Accessor for TestAccessor {
        fn reads(&self) -> &[ComponentTypeId] {
            &self.reads
        }
        fn writes(&self) -> &[ComponentTypeId] {
            &self.writes
        }
    }

    type TestBarrier = u32;
    impl Barrier for TestBarrier {}

    #[derive(Debug)]
    struct TestJob {
        id: u32,
        accessor: TestAccessor,
        run_after: Option<TestBarrier>,
        finish_before: Option<TestBarrier>,
    }
    impl JobDescriptor for TestJob {
        type Accessor = TestAccessor;
        type Barrier = TestBarrier;
        fn accessor(&self) -> &Self::Accessor {
            &self.accessor
        }
        fn run_after(&self) -> Option<Self::Barrier> {
            self.run_after
        }
        fn finish_before(&self) -> Option<Self::Barrier> {
            self.finish_before
        }
    }

    fn type_id(id: u32) -> ComponentTypeId {
        ComponentTypeId(std::any::TypeId::of::<u32>(), id)
    }
    fn accessor(reads: Vec<ComponentTypeId>, writes: Vec<ComponentTypeId>) -> TestAccessor {
        TestAccessor { reads, writes }
    }

    fn generate_test_jobs() -> Vec<TestJob> {
        let job1 = TestJob {
            id: 1,
            accessor: accessor(vec![], vec![type_id(3)]),
            run_after: None,
            finish_before: None,
        };
        let job2 = TestJob {
            id: 2,
            accessor: accessor(vec![type_id(3)], vec![type_id(4)]),
            run_after: Some(2),
            finish_before: None,
        };
        let job3 = TestJob {
            id: 3,
            accessor: accessor(vec![type_id(3)], vec![type_id(5)]),
            run_after: Some(2),
            finish_before: None,
        };
        let job4 = TestJob {
            id: 4,
            accessor: accessor(vec![], vec![type_id(3)]),
            run_after: Some(1),
            finish_before: None,
        };
        let job5 = TestJob {
            id: 5,
            accessor: accessor(vec![], vec![type_id(3)]),
            run_after: Some(0),
            finish_before: Some(2),
        };
        let job6 = TestJob {
            id: 6,
            accessor: accessor(vec![], vec![type_id(3)]),
            run_after: Some(1),
            finish_before: Some(2),
        };
        vec![job6, job5, job4, job3, job2, job1]
    }
    #[test]
    fn generate_graph() {
        let jobs = generate_test_jobs();
        let graph = generate_job_graph(&jobs);
        dbg!(&graph);
        let ordering: Vec<_> = petgraph::algo::toposort(&graph, None)
            .unwrap()
            .into_iter()
            .map(|n| &graph[n])
            .collect();
        dbg!(ordering);
    }

    #[test]
    fn dispatch_state() {
        let jobs = generate_test_jobs();
        let graph = generate_job_graph(&jobs);
        let dispatch_state = build_dispatch_state(&graph);
        for (idx, dep_list) in dispatch_state.job_deps.iter().enumerate() {
            println!(
                "deps for job {:#?}: {:#?}",
                dispatch_state.sorted_jobs[idx],
                dep_list
                    .into_iter()
                    .map(|d| dispatch_state.sorted_jobs[d as usize])
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn dispatch_schedule() {
        let jobs = generate_test_jobs();
        let graph = generate_job_graph(&jobs);
        let mut dispatch_state = build_dispatch_state(&graph);
        let mut schedule_order = Vec::new();
        let mut complete_queue = Vec::new();
        loop {
            match dispatch_state.next_job() {
                ScheduleResult::Done => break,
                ScheduleResult::WaitingForJob => {
                    if let Some(job) = complete_queue.pop() {
                        dispatch_state.complete_job(job);
                    } else {
                        assert!(false, "Waiting for job while scheduling");
                    }
                }
                ScheduleResult::Schedule(job, idx) => {
                    println!("schedule {:#?}", job);
                    schedule_order.push(job);
                    complete_queue.push(idx);
                    dispatch_state.complete_job(idx)
                }
            }
        }
        dbg!(schedule_order);
    }

}
