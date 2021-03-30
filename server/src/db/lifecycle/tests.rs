use super::*;
use std::num::{NonZeroU32, NonZeroUsize};

fn from_secs(secs: i64) -> DateTime<Utc> {
    DateTime::from_utc(chrono::NaiveDateTime::from_timestamp(secs, 0), Utc)
}

fn new_chunk(id: u32, time_of_first_write: Option<i64>, time_of_last_write: Option<i64>) -> Chunk {
    let mut chunk = Chunk::new_open("", id);
    chunk.set_timestamps(
        time_of_first_write.map(from_secs),
        time_of_last_write.map(from_secs),
    );
    chunk
}

#[derive(Debug, Eq, PartialEq)]
enum MoverEvents {
    Move(u32),
    Drop(u32),
}

/// A dummy mover that is used to test the policy
/// logic within ChunkMover::poll
struct DummyMover {
    rules: LifecycleRules,
    move_active: bool,
    chunks: Vec<Arc<RwLock<Chunk>>>,
    events: Vec<MoverEvents>,
}

impl DummyMover {
    fn new(rules: LifecycleRules, chunks: Vec<Chunk>) -> Self {
        Self {
            rules,
            chunks: chunks
                .into_iter()
                .map(|x| Arc::new(RwLock::new(x)))
                .collect(),
            move_active: false,
            events: vec![],
        }
    }
}

impl ChunkMover for DummyMover {
    fn chunk_size(_: &Chunk) -> usize {
        // All chunks are 20 bytes
        20
    }

    fn rules(&self) -> &LifecycleRules {
        &self.rules
    }

    fn chunks(&self) -> Vec<Arc<RwLock<Chunk>>> {
        self.chunks.clone()
    }

    fn is_move_active(&self) -> bool {
        self.move_active
    }

    fn move_to_read_buffer(&mut self, _: String, chunk_id: u32) {
        let chunk = self
            .chunks
            .iter()
            .find(|x| x.read().id() == chunk_id)
            .unwrap();
        chunk.write().set_moving().unwrap();
        self.events.push(MoverEvents::Move(chunk_id))
    }

    fn drop_chunk(&mut self, _: String, chunk_id: u32) {
        self.events.push(MoverEvents::Drop(chunk_id))
    }
}

#[test]
fn test_elapsed_seconds() {
    assert_eq!(elapsed_seconds(from_secs(10), from_secs(5)), 5);
    assert_eq!(elapsed_seconds(from_secs(10), from_secs(10)), 0);
    assert_eq!(elapsed_seconds(from_secs(10), from_secs(15)), 0);
}

#[test]
fn test_can_move() {
    // Cannot move by default
    let rules = LifecycleRules::default();
    let chunk = new_chunk(0, Some(0), Some(0));
    assert!(!can_move(&rules, &chunk, from_secs(20)));

    // If only mutable_linger set can move a chunk once passed
    let rules = LifecycleRules {
        mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
        ..Default::default()
    };
    let chunk = new_chunk(0, Some(0), Some(0));
    assert!(!can_move(&rules, &chunk, from_secs(9)));
    assert!(can_move(&rules, &chunk, from_secs(11)));

    // If mutable_minimum_age_seconds set must also take this into account
    let rules = LifecycleRules {
        mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
        mutable_minimum_age_seconds: Some(NonZeroU32::new(60).unwrap()),
        ..Default::default()
    };
    let chunk = new_chunk(0, Some(0), Some(0));
    assert!(!can_move(&rules, &chunk, from_secs(9)));
    assert!(!can_move(&rules, &chunk, from_secs(11)));
    assert!(can_move(&rules, &chunk, from_secs(61)));

    let chunk = new_chunk(0, Some(0), Some(70));
    assert!(!can_move(&rules, &chunk, from_secs(71)));
    assert!(can_move(&rules, &chunk, from_secs(81)));
}

#[test]
fn test_default_rules() {
    // The default rules shouldn't do anything
    let rules = LifecycleRules::default();
    let chunks = vec![
        new_chunk(0, Some(1), Some(1)),
        new_chunk(1, Some(20), Some(1)),
        new_chunk(2, Some(30), Some(1)),
    ];

    let mut mover = DummyMover::new(rules, chunks);
    mover.poll(from_secs(40));
    assert_eq!(mover.events, vec![]);
}

#[test]
fn test_mutable_linger() {
    let rules = LifecycleRules {
        mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
        ..Default::default()
    };
    let chunks = vec![
        new_chunk(0, Some(0), Some(8)),
        new_chunk(1, Some(0), Some(5)),
        new_chunk(2, Some(0), Some(0)),
    ];

    let mut mover = DummyMover::new(rules, chunks);
    mover.poll(from_secs(9));

    assert_eq!(mover.events, vec![]);

    mover.poll(from_secs(11));
    assert_eq!(mover.events, vec![MoverEvents::Move(2)]);

    mover.poll(from_secs(12));
    assert_eq!(mover.events, vec![MoverEvents::Move(2)]);

    // Should move in the order of chunks in the case of multiple candidates
    mover.poll(from_secs(20));
    assert_eq!(
        mover.events,
        vec![MoverEvents::Move(2), MoverEvents::Move(0)]
    );

    mover.poll(from_secs(20));

    assert_eq!(
        mover.events,
        vec![
            MoverEvents::Move(2),
            MoverEvents::Move(0),
            MoverEvents::Move(1)
        ]
    );
}

#[test]
fn test_in_progress() {
    let rules = LifecycleRules {
        mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
        ..Default::default()
    };
    let chunks = vec![new_chunk(0, Some(0), Some(0))];

    let mut mover = DummyMover::new(rules, chunks);
    mover.move_active = true;

    mover.poll(from_secs(80));

    assert_eq!(mover.events, vec![]);

    mover.move_active = false;

    mover.poll(from_secs(80));

    assert_eq!(mover.events, vec![MoverEvents::Move(0)]);
}

#[test]
fn test_minimum_age() {
    let rules = LifecycleRules {
        mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
        mutable_minimum_age_seconds: Some(NonZeroU32::new(60).unwrap()),
        ..Default::default()
    };
    let chunks = vec![
        new_chunk(0, Some(40), Some(40)),
        new_chunk(1, Some(0), Some(0)),
    ];

    let mut mover = DummyMover::new(rules, chunks);

    mover.poll(from_secs(80));

    assert_eq!(mover.events, vec![MoverEvents::Move(1)]);

    mover.poll(from_secs(90));

    assert_eq!(mover.events, vec![MoverEvents::Move(1)]);

    mover.poll(from_secs(110));

    assert_eq!(
        mover.events,
        vec![MoverEvents::Move(1), MoverEvents::Move(0)]
    );
}

#[test]
fn test_buffer_size_soft() {
    let rules = LifecycleRules {
        buffer_size_soft: Some(NonZeroUsize::new(5).unwrap()),
        ..Default::default()
    };

    let rb = Arc::new(read_buffer::Database::new());

    let chunks = vec![new_chunk(0, Some(0), Some(0))];

    let mut mover = DummyMover::new(rules.clone(), chunks);

    mover.poll(from_secs(10));
    assert_eq!(mover.events, vec![]);

    let mut chunks = vec![
        new_chunk(0, Some(0), Some(0)),
        new_chunk(1, Some(0), Some(0)),
        new_chunk(2, Some(0), Some(0)),
    ];

    chunks[2].set_closing().unwrap();
    chunks[2].set_moving().unwrap();
    chunks[2].set_moved(Arc::clone(&rb)).unwrap();

    let mut mover = DummyMover::new(rules, chunks);

    mover.poll(from_secs(10));
    assert_eq!(mover.events, vec![MoverEvents::Drop(2)]);

    let rules = LifecycleRules {
        buffer_size_soft: Some(NonZeroUsize::new(40).unwrap()),
        ..Default::default()
    };

    let chunks = vec![new_chunk(0, Some(0), Some(0))];

    let mut mover = DummyMover::new(rules, chunks);

    mover.poll(from_secs(10));
    assert_eq!(mover.events, vec![]);
}
