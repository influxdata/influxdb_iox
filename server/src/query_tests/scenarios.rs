//! This module contains testing scenarios for Db

#[allow(unused_imports, dead_code, unused_macros)]
use query::PartitionChunk;

use async_trait::async_trait;

use crate::{db::test_helpers::write_lp, Db};

use super::utils::{
    count_mutable_buffer_chunks, count_object_store_chunks, count_read_buffer_chunks, make_db,
};

/// Holds a database and a description of how its data was configured
#[derive(Debug)]
pub struct DbScenario {
    pub scenario_name: String,
    pub db: Db,
}

#[async_trait]
pub trait DbSetup {
    // Create several scenarios, scenario has the same data, but
    // different physical arrangements (e.g.  the data is in different chunks)
    async fn make(&self) -> Vec<DbScenario>;
}

/// No data
#[derive(Debug)]
pub struct NoData {}
#[async_trait]
impl DbSetup for NoData {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";

        // Scenario 1: No data in the DB yet
        //
        let db = make_db().await.db;
        let scenario1 = DbScenario {
            scenario_name: "New, Empty Database".into(),
            db,
        };

        // Scenario 2: listing partitions (which may create an entry in a map)
        // in an empty database
        //
        let db = make_db().await.db;
        assert_eq!(count_mutable_buffer_chunks(&db), 0);
        assert_eq!(count_read_buffer_chunks(&db), 0);
        assert_eq!(count_object_store_chunks(&db), 0);
        let scenario2 = DbScenario {
            scenario_name: "New, Empty Database after partitions are listed".into(),
            db,
        };

        // Scenario 3: the database has had data loaded into RB and then deleted
        //
        let db = make_db().await.db;
        let data = "cpu,region=west user=23.2 100";
        write_lp(&db, data);
        // move data out of open chunk
        assert_eq!(
            db.rollover_partition(partition_key, table_name)
                .await
                .unwrap()
                .unwrap()
                .id(),
            0
        );
        assert_eq!(count_mutable_buffer_chunks(&db), 1); //
        assert_eq!(count_read_buffer_chunks(&db), 0); // nothing yet
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // Now load the closed chunk into the RB
        db.load_chunk_to_read_buffer(partition_key, table_name, 0)
            .await
            .unwrap();
        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 1); // close chunk only
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // drop chunk 0
        db.drop_chunk(partition_key, table_name, 0).unwrap();

        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 0); // nothing after dropping chunk 0
        assert_eq!(count_object_store_chunks(&db), 0); // still nothing

        let scenario3 = DbScenario {
            scenario_name: "Empty Database after drop chunk that is in read buffer".into(),
            db,
        };

        // Scenario 4: the database has had data loaded into RB & Object Store and then deleted
        //
        let db = make_db().await.db;
        let data = "cpu,region=west user=23.2 100";
        write_lp(&db, data);
        // move data out of open chunk
        assert_eq!(
            db.rollover_partition(partition_key, table_name)
                .await
                .unwrap()
                .unwrap()
                .id(),
            0
        );
        assert_eq!(count_mutable_buffer_chunks(&db), 1); // 1 open chunk
        assert_eq!(count_read_buffer_chunks(&db), 0); // nothing yet
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // Now load the closed chunk into the RB
        db.load_chunk_to_read_buffer(partition_key, table_name, 0)
            .await
            .unwrap();
        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 1); // close chunk only
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // Now write the data in RB to object store but keep it in RB
        db.write_chunk_to_object_store(partition_key, "cpu", 0)
            .await
            .unwrap();
        // it should be the same chunk!
        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 1); // closed chunk only
        assert_eq!(count_object_store_chunks(&db), 1); // close chunk only

        // drop chunk 0
        db.drop_chunk(partition_key, table_name, 0).unwrap();

        assert_eq!(count_mutable_buffer_chunks(&db), 0);
        assert_eq!(count_read_buffer_chunks(&db), 0);
        assert_eq!(count_object_store_chunks(&db), 0);

        let scenario4 = DbScenario {
            scenario_name:
                "Empty Database after drop chunk that is in both read buffer and object store"
                    .into(),
            db,
        };

        vec![scenario1, scenario2, scenario3, scenario4]
    }
}

/// Two measurements data in a single mutable buffer chunk
#[derive(Debug)]
pub struct TwoMeasurements {}
#[async_trait]
impl DbSetup for TwoMeasurements {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "cpu,region=west user=23.2 100",
            "cpu,region=west user=21.0 150",
            "disk,region=east bytes=99i 200",
        ];

        make_one_chunk_scenarios(partition_key, &lp_lines.join("\n")).await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsUnsignedType {}
#[async_trait]
impl DbSetup for TwoMeasurementsUnsignedType {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "restaurant,town=andover count=40000u 100",
            "restaurant,town=reading count=632u 120",
            "school,town=reading count=17u 150",
            "school,town=andover count=25u 160",
        ];

        make_one_chunk_scenarios(partition_key, &lp_lines.join("\n")).await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsPredicatePushDown {}
#[async_trait]
impl DbSetup for TwoMeasurementsPredicatePushDown {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "restaurant,town=andover count=40000u,system=5.0 100",
            "restaurant,town=reading count=632u,system=5.0 120",
            "restaurant,town=bedford count=189u,system=7.0 110",
            "restaurant,town=tewsbury count=471u,system=6.0 110",
            "restaurant,town=lexington count=372u,system=5.0 100",
            "restaurant,town=lawrence count=872u,system=6.0 110",
            "restaurant,town=reading count=632u,system=6.0 130",
            "school,town=reading count=17u,system=6.0 150",
            "school,town=andover count=25u,system=6.0 160",
        ];

        make_one_rub_chunk_scenario(partition_key, &lp_lines.join("\n")).await
    }
}

/// Single measurement that has several different chunks with
/// different (but compatible) schema
#[derive(Debug)]
pub struct MultiChunkSchemaMerge {}
#[async_trait]
impl DbSetup for MultiChunkSchemaMerge {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "cpu,region=west user=23.2,system=5.0 100",
            "cpu,region=west user=21.0,system=6.0 150",
        ];
        let lp_lines2 = [
            "cpu,region=east,host=foo user=23.2 100",
            "cpu,region=west,host=bar user=21.0 250",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

/// Two measurements data with many null values
#[derive(Debug)]
pub struct TwoMeasurementsManyNulls {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyNulls {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=CA,city=LA,county=LA temp=70.4 100",
            "h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250",
            "o2,state=MA,city=Boston temp=50.4 200",
            "o2,state=CA temp=79.0 300\n",
        ];
        let lp_lines2 = [
            "o2,state=NY temp=60.8 400",
            "o2,state=NY,city=NYC temp=61.0 500",
            "o2,state=NY,city=NYC,borough=Brooklyn temp=61.0 600",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsManyFields {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyFields {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
            "o2,state=CA temp=79.0 300",
        ];
        let lp_lines2 = vec!["h2o,state=MA,city=Boston temp=70.4,moisture=43.0 100000"];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[derive(Debug)]
/// This has a single chunk for queries that check the state of the system
pub struct TwoMeasurementsManyFieldsOneChunk {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyFieldsOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        let db = make_db().await.db;

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
            "o2,state=CA temp=79.0 300",
        ];

        write_lp(&db, &lp_lines.join("\n"));
        vec![DbScenario {
            scenario_name: "Data in open chunk of mutable buffer".into(),
            db,
        }]
    }
}

#[derive(Debug)]
/// This has two chunks for queries that check the state of the system
pub struct TwoMeasurementsManyFieldsTwoChunks {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyFieldsTwoChunks {
    async fn make(&self) -> Vec<DbScenario> {
        let db = make_db().await.db;

        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
        ];
        write_lp(&db, &lp_lines.join("\n"));
        db.rollover_partition(partition_key, "h2o").await.unwrap();
        db.load_chunk_to_read_buffer(partition_key, "h2o", 0)
            .await
            .unwrap();

        let lp_lines = vec![
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
            "o2,state=CA temp=79.0 300",
        ];
        write_lp(&db, &lp_lines.join("\n"));

        vec![DbScenario {
            scenario_name: "Data in open chunk of mutable buffer and read buffer".into(),
            db,
        }]
    }
}

#[derive(Debug)]
/// This has a single scenario with all the life cycle operations to
/// test queries that depend on that
pub struct TwoMeasurementsManyFieldsLifecycle {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyFieldsLifecycle {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let db = std::sync::Arc::new(make_db().await.db);

        write_lp(
            &db,
            &vec![
                "h2o,state=MA,city=Boston temp=70.4 50",
                "h2o,state=MA,city=Boston other_temp=70.4 250",
            ]
            .join("\n"),
        );

        // Use a background task to do the work note when I used
        // TaskTracker::join, it ended up hanging for reasons I don't
        // now
        let job = db.load_chunk_to_read_buffer_in_background(
            partition_key.to_string(),
            "h2o".to_string(),
            0,
        );
        job.join().await;

        write_lp(
            &db,
            &vec!["h2o,state=CA,city=Boston other_temp=72.4 350"].join("\n"),
        );

        let job = db.write_chunk_to_object_store_in_background(
            partition_key.to_string(),
            "h2o".to_string(),
            0,
        );
        job.join().await;

        let db =
            std::sync::Arc::try_unwrap(db).expect("All background handles to db should be done");

        vec![DbScenario {
            scenario_name: "Data in parquet, and MUB".into(),
            db,
        }]
    }
}

#[derive(Debug)]
pub struct OneMeasurementManyFields {}
#[async_trait]
impl DbSetup for OneMeasurementManyFields {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        // Order this so field3 comes before field2
        // (and thus the columns need to get reordered)
        let lp_lines = vec![
            "h2o,tag1=foo,tag2=bar field1=70.6,field3=2 100",
            "h2o,tag1=foo,tag2=bar field1=70.4,field2=\"ss\" 100",
            "h2o,tag1=foo,tag2=bar field1=70.5,field2=\"ss\" 100",
            "h2o,tag1=foo,tag2=bar field1=70.6,field4=true 1000",
        ];

        make_one_chunk_scenarios(partition_key, &lp_lines.join("\n")).await
    }
}

/// This data (from end to end test)
#[derive(Debug)]
pub struct EndToEndTest {}
#[async_trait]
impl DbSetup for EndToEndTest {
    async fn make(&self) -> Vec<DbScenario> {
        let lp_lines = vec![
            "cpu_load_short,host=server01,region=us-west value=0.64 0000",
            "cpu_load_short,host=server01 value=27.99 1000",
            "cpu_load_short,host=server02,region=us-west value=3.89 2000",
            "cpu_load_short,host=server01,region=us-east value=1234567.891011 3000",
            "cpu_load_short,host=server01,region=us-west value=0.000003 4000",
            "system,host=server03 uptime=1303385 5000",
            "swap,host=server01,name=disk0 in=3,out=4 6000",
            "status active=t 7000",
            "attributes color=\"blue\" 8000",
        ];

        let lp_data = lp_lines.join("\n");

        let db = make_db().await.db;
        write_lp(&db, &lp_data);

        let scenario1 = DbScenario {
            scenario_name: "Data in open chunk of mutable buffer".into(),
            db,
        };
        vec![scenario1]
    }
}

/// This function loads one chunk of lp data into different scenarios that simulates
/// the data life cycle.
///
pub(crate) async fn make_one_chunk_scenarios(partition_key: &str, data: &str) -> Vec<DbScenario> {
    // Scenario 1: One open chunk in MUB
    let db = make_db().await.db;
    write_lp(&db, data);
    let scenario1 = DbScenario {
        scenario_name: "Data in open chunk of mutable buffer".into(),
        db,
    };

    // Scenario 2: One closed chunk in MUB
    let db = make_db().await.db;
    let table_names = write_lp(&db, data);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();
    }
    let scenario2 = DbScenario {
        scenario_name: "Data in closed chunk of mutable buffer".into(),
        db,
    };

    // Scenario 3: One closed chunk in RUB
    let db = make_db().await.db;
    let table_names = write_lp(&db, data);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();
        db.load_chunk_to_read_buffer(partition_key, &table_name, 0)
            .await
            .unwrap();
    }
    let scenario3 = DbScenario {
        scenario_name: "Data in read buffer".into(),
        db,
    };

    // Scenario 4: One closed chunk in both RUb and OS
    let db = make_db().await.db;
    let table_names = write_lp(&db, data);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();
        db.load_chunk_to_read_buffer(partition_key, &table_name, 0)
            .await
            .unwrap();
        db.write_chunk_to_object_store(partition_key, &table_name, 0)
            .await
            .unwrap();
    }
    let scenario4 = DbScenario {
        scenario_name: "Data in both read buffer and object store".into(),
        db,
    };

    // Scenario 5: One closed chunk in OS only
    let db = make_db().await.db;
    let table_names = write_lp(&db, data);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();
        db.load_chunk_to_read_buffer(partition_key, &table_name, 0)
            .await
            .unwrap();
        db.write_chunk_to_object_store(partition_key, &table_name, 0)
            .await
            .unwrap();
        db.unload_read_buffer(partition_key, &table_name, 0)
            .await
            .unwrap();
    }
    let scenario5 = DbScenario {
        scenario_name: "Data in object store only".into(),
        db,
    };

    vec![scenario1, scenario2, scenario3, scenario4, scenario5]
}

/// This function loads two chunks of lp data into 4 different scenarios
///
/// Data in single open mutable buffer chunk
/// Data in one open mutable buffer chunk, one closed mutable chunk
/// Data in one open mutable buffer chunk, one read buffer chunk
/// Data in one two read buffer chunks,
pub async fn make_two_chunk_scenarios(
    partition_key: &str,
    data1: &str,
    data2: &str,
) -> Vec<DbScenario> {
    let db = make_db().await.db;
    write_lp(&db, data1);
    write_lp(&db, data2);
    let scenario1 = DbScenario {
        scenario_name: "Data in single open chunk of mutable buffer".into(),
        db,
    };

    // spread across 2 mutable buffer chunks
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();
    }
    write_lp(&db, data2);
    let scenario2 = DbScenario {
        scenario_name: "Data in one open chunk and one closed chunk of mutable buffer".into(),
        db,
    };

    // spread across 1 mutable buffer, 1 read buffer chunks
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();
        db.load_chunk_to_read_buffer(partition_key, &table_name, 0)
            .await
            .unwrap();
    }
    write_lp(&db, data2);
    let scenario3 = DbScenario {
        scenario_name: "Data in open chunk of mutable buffer, and one chunk of read buffer".into(),
        db,
    };

    // in 2 read buffer chunks
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();
    }
    let table_names = write_lp(&db, data2);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();

        db.load_chunk_to_read_buffer(partition_key, &table_name, 0)
            .await
            .unwrap();

        db.load_chunk_to_read_buffer(partition_key, &table_name, 1)
            .await
            .unwrap();
    }
    let scenario4 = DbScenario {
        scenario_name: "Data in two read buffer chunks".into(),
        db,
    };

    // in 2 read buffer chunks that also loaded into object store
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();
    }
    let table_names = write_lp(&db, data2);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();

        db.load_chunk_to_read_buffer(partition_key, &table_name, 0)
            .await
            .unwrap();

        db.load_chunk_to_read_buffer(partition_key, &table_name, 1)
            .await
            .unwrap();

        db.write_chunk_to_object_store(partition_key, &table_name, 0)
            .await
            .unwrap();

        db.write_chunk_to_object_store(partition_key, &table_name, 1)
            .await
            .unwrap();
    }
    let scenario5 = DbScenario {
        scenario_name: "Data in two read buffer chunks and two parquet file chunks".into(),
        db,
    };

    // Scenario 6: Two closed chunk in OS only
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();
    }
    let table_names = write_lp(&db, data2);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();

        db.load_chunk_to_read_buffer(partition_key, &table_name, 0)
            .await
            .unwrap();

        db.load_chunk_to_read_buffer(partition_key, &table_name, 1)
            .await
            .unwrap();

        db.write_chunk_to_object_store(partition_key, &table_name, 0)
            .await
            .unwrap();

        db.write_chunk_to_object_store(partition_key, &table_name, 1)
            .await
            .unwrap();
        db.unload_read_buffer(partition_key, &table_name, 0)
            .await
            .unwrap();
        db.unload_read_buffer(partition_key, &table_name, 1)
            .await
            .unwrap();
    }
    let scenario6 = DbScenario {
        scenario_name: "Data in 2 parquet chunks in object store only".into(),
        db,
    };

    vec![
        scenario1, scenario2, scenario3, scenario4, scenario5, scenario6,
    ]
}

/// Rollover the mutable buffer and load chunk 0 to the read buffer and object store
pub async fn rollover_and_load(db: &Db, partition_key: &str, table_name: &str) {
    db.rollover_partition(partition_key, table_name)
        .await
        .unwrap();
    db.load_chunk_to_read_buffer(partition_key, table_name, 0)
        .await
        .unwrap();
    db.write_chunk_to_object_store(partition_key, table_name, 0)
        .await
        .unwrap();
}

// This function loads one chunk of lp data into RUB for testing predicate pushdown
pub(crate) async fn make_one_rub_chunk_scenario(
    partition_key: &str,
    data: &str,
) -> Vec<DbScenario> {
    // Scenario 1: One closed chunk in RUB
    let db = make_db().await.db;
    let table_names = write_lp(&db, data);
    for table_name in &table_names {
        db.rollover_partition(partition_key, &table_name)
            .await
            .unwrap();
        db.load_chunk_to_read_buffer(partition_key, &table_name, 0)
            .await
            .unwrap();
    }
    let scenario1 = DbScenario {
        scenario_name: "Data in read buffer".into(),
        db,
    };

    vec![scenario1]
}
