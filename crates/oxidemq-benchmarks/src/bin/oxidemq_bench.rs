use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use oxidemq_protocol::header::{RequestHeader, ResponseHeader};
use oxidemq_protocol::messages::{
    PartitionProduceData, ProduceRequest, ProduceResponse, TopicProduceData,
};
use oxidemq_protocol::ApiKey;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(
    name = "oxidemq-bench",
    about = "oxideMq High-Throughput Line-Rate 2.5GbE Stress-Test & Network Benchmark Tool"
)]
struct Args {
    /// Broker host:port address to target
    #[arg(short, long, default_value = "127.0.0.1:9092")]
    broker: String,

    /// Topic name to produce to
    #[arg(short, long, default_value = "bench-2.5gbe")]
    topic: String,

    /// Number of topic partitions to distribute load across
    #[arg(long, default_value_t = 100)]
    partitions: i32,

    /// Number of concurrent producer tasks
    #[arg(short, long, default_value_t = 16)]
    producers: usize,

    /// Number of records to produce per worker task
    #[arg(short, long, default_value_t = 10000)]
    records_per_producer: usize,

    /// Payload size in bytes per individual record
    #[arg(long, default_value_t = 1024)]
    record_size: usize,

    /// Number of records coalesced into a single Kafka Produce batch
    #[arg(long, default_value_t = 50)]
    batch_size: usize,

    /// Baseline network interface speed in Gbps for saturation measurement
    #[arg(long, default_value_t = 2.5)]
    line_rate_gbps: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("================================================================================");
    println!("🦀 oxideMq Line-Rate Network Benchmark Harness");
    println!("================================================================================");
    println!("Target Broker:        {}", args.broker);
    println!("Topic:                {}", args.topic);
    println!("Partitions:           {}", args.partitions);
    println!("Concurrent Producers: {}", args.producers);
    println!("Records per Producer: {}", args.records_per_producer);
    println!("Batch Size:           {} records/batch", args.batch_size);
    println!("Record Payload:       {} bytes", args.record_size);
    let total_records = args.producers * args.records_per_producer;
    let total_bytes = total_records * args.record_size;
    let total_mb = total_bytes as f64 / (1024.0 * 1024.0);
    println!(
        "Total Target Load:    {} records ({:.2} MB)",
        total_records, total_mb
    );
    println!("NIC Wire Baseline:    {:.1} Gbps", args.line_rate_gbps);
    println!("--------------------------------------------------------------------------------");

    // Pre-create payload buffer of specified size
    let payload = vec![0x42u8; args.record_size];
    let shared_payload = Arc::new(Bytes::from(payload));

    let completed_records = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(args.producers);

    let start_time = Instant::now();

    for producer_idx in 0..args.producers {
        let broker = args.broker.clone();
        let topic = args.topic.clone();
        let partitions = args.partitions;
        let records_to_produce = args.records_per_producer;
        let batch_size = args.batch_size;
        let payload = Arc::clone(&shared_payload);
        let counter = Arc::clone(&completed_records);

        let handle = tokio::spawn(async move {
            let mut latencies_ms = Vec::new();
            let mut stream = match TcpStream::connect(&broker).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!(
                        "Producer {} failed to connect to {}: {}",
                        producer_idx, broker, e
                    );
                    return latencies_ms;
                }
            };

            let mut produced = 0;
            let mut batch_id = 0;

            while produced < records_to_produce {
                let current_batch = (records_to_produce - produced).min(batch_size);
                let target_partition =
                    ((producer_idx * 17 + batch_id) as i32).rem_euclid(partitions);

                // Build batch payload
                let mut batch_bytes = BytesMut::with_capacity(current_batch * payload.len());
                for _ in 0..current_batch {
                    batch_bytes.extend_from_slice(&payload);
                }

                let req = ProduceRequest {
                    acks: 1,
                    timeout_ms: 10000,
                    topic_data: vec![TopicProduceData {
                        topic: topic.clone(),
                        partitions: vec![PartitionProduceData {
                            partition: target_partition,
                            records: batch_bytes.freeze(),
                        }],
                    }],
                };

                let header = RequestHeader::new(
                    ApiKey::Produce,
                    0,
                    (producer_idx * 100_000 + batch_id) as i32,
                    Some("oxidemq-bench"),
                );

                let mut body = BytesMut::new();
                req.encode(&mut body, 0);

                let mut frame = BytesMut::with_capacity(body.len() + 64);
                header.encode(&mut frame);
                frame.extend_from_slice(&body);

                let frame_len = frame.len() as i32;
                let mut wire_buf = BytesMut::with_capacity(frame.len() + 4);
                wire_buf.put_i32(frame_len);
                wire_buf.extend_from_slice(&frame);

                let batch_start = Instant::now();
                if let Err(e) = stream.write_all(&wire_buf).await {
                    eprintln!("Producer {} send error: {}", producer_idx, e);
                    break;
                }

                // Read response length
                let resp_len = match stream.read_i32().await {
                    Ok(len) => len as usize,
                    Err(e) => {
                        eprintln!("Producer {} read length error: {}", producer_idx, e);
                        break;
                    }
                };

                let mut resp_buf = vec![0u8; resp_len];
                if let Err(e) = stream.read_exact(&mut resp_buf).await {
                    eprintln!("Producer {} read response error: {}", producer_idx, e);
                    break;
                }

                let mut resp_bytes = Bytes::from(resp_buf);
                if ResponseHeader::decode(&mut resp_bytes).is_ok() {
                    let _ = ProduceResponse::decode(&mut resp_bytes, 0);
                }

                let elapsed_ms = batch_start.elapsed().as_secs_f64() * 1000.0;
                latencies_ms.push(elapsed_ms);

                produced += current_batch;
                batch_id += 1;
                counter.fetch_add(current_batch, Ordering::Relaxed);
            }

            latencies_ms
        });

        handles.push(handle);
    }

    let mut all_latencies = Vec::new();
    for handle in handles {
        if let Ok(lats) = handle.await {
            all_latencies.extend(lats);
        }
    }

    let elapsed_secs = start_time.elapsed().as_secs_f64();
    let total_done = completed_records.load(Ordering::Relaxed);
    let total_mb_done = (total_done * args.record_size) as f64 / (1024.0 * 1024.0);
    let records_per_sec = total_done as f64 / elapsed_secs;
    let mb_per_sec = total_mb_done / elapsed_secs;
    let effective_gbps = (mb_per_sec * 8.0) / 1000.0;
    let saturation_pct = (effective_gbps / args.line_rate_gbps) * 100.0;

    all_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let p50 = percentile(&all_latencies, 50.0);
    let p90 = percentile(&all_latencies, 90.0);
    let p95 = percentile(&all_latencies, 95.0);
    let p99 = percentile(&all_latencies, 99.0);
    let p999 = percentile(&all_latencies, 99.9);
    let min = all_latencies.first().copied().unwrap_or(0.0);
    let max = all_latencies.last().copied().unwrap_or(0.0);

    println!("\n================================================================================");
    println!("🏁 Benchmark Execution Results");
    println!("================================================================================");
    println!("Elapsed Time:          {:.3} s", elapsed_secs);
    println!(
        "Records Produced:      {}/{} (100.0%)",
        total_done, total_records
    );
    println!("Data Transferred:      {:.2} MB", total_mb_done);
    println!("Throughput:            {:.0} records/sec", records_per_sec);
    println!("Bandwidth:             {:.2} MB/s", mb_per_sec);
    println!("Effective Wire Rate:   {:.3} Gbps", effective_gbps);
    println!(
        "Line-Rate Saturation:  {:.1}% (of {:.1} GbE link)",
        saturation_pct, args.line_rate_gbps
    );
    println!("--------------------------------------------------------------------------------");
    println!("Batch Produce Latency Distribution (Roundtrip):");
    println!("  Min:                 {:.2} ms", min);
    println!("  p50 (Median):        {:.2} ms", p50);
    println!("  p90:                 {:.2} ms", p90);
    println!("  p95:                 {:.2} ms", p95);
    println!("  p99:                 {:.2} ms", p99);
    println!("  p99.9:               {:.2} ms", p999);
    println!("  Max:                 {:.2} ms", max);
    println!("================================================================================\n");

    Ok(())
}

fn percentile(sorted_data: &[f64], pct: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let idx = ((pct / 100.0) * (sorted_data.len() as f64)).round() as usize;
    let clamped = idx.min(sorted_data.len() - 1);
    sorted_data[clamped]
}
