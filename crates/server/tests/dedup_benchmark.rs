mod helpers;

use std::time::Instant;

use helpers::{TestServer, build_upload_artifacts, generate_test_data, upload_artifacts};

/// Benchmark: upload a "model", tweak ~5% of weights, re-upload.
/// Shows bytes transferred and timing for each, proving dedup works.
///
/// Needs >60MB to span multiple xorbs (XORB_SOFT_LIMIT = 60MB).
/// We use 200MB so we get 3-4 xorbs and clear dedup signal.
#[tokio::test]
async fn test_dedup_benchmark() {
    let model_size = 200 * 1024 * 1024; // 200 MB
    let original = generate_test_data(model_size);

    // Tweak ~5% of the data in one contiguous region (simulates
    // fine-tuning a few layers — weight changes are localized)
    let mut tweaked = original.clone();
    let tweak_size = model_size / 20; // 5% = 10 MB
    let tweak_start = model_size / 3; // offset into the middle
    for i in 0..tweak_size {
        tweaked[tweak_start + i] = tweaked[tweak_start + i].wrapping_add(1);
    }

    let server = TestServer::start().await;

    // ── Upload original ──────────────────────────────────────────────────
    let t0 = Instant::now();
    let original_artifacts = build_upload_artifacts(&original);
    let chunk_time = t0.elapsed();

    let original_xorb_bytes: usize = original_artifacts
        .xorb_entries
        .iter()
        .map(|(_, b)| b.len())
        .sum();

    let t1 = Instant::now();
    upload_artifacts(&server, &original_artifacts).await;
    let upload_time = t1.elapsed();

    eprintln!("─── Original upload ({} MB) ───", model_size / 1_048_576);
    eprintln!(
        "  Chunks:       {}",
        original_artifacts.chunk_hashes.len()
    );
    eprintln!("  Xorbs:        {}", original_artifacts.xorb_entries.len());
    eprintln!(
        "  Bytes sent:   {} MB ({} bytes)",
        original_xorb_bytes / 1_048_576,
        original_xorb_bytes
    );
    eprintln!("  Chunk time:   {:?}", chunk_time);
    eprintln!("  Upload time:  {:?}", upload_time);

    // Verify reconstruction works
    let token = server.read_token();
    let resp = server
        .client
        .get(format!(
            "{}/api/files/{}/content",
            server.base_url, original_artifacts.file_hash
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let downloaded = resp.bytes().await.unwrap();
    assert_eq!(downloaded.len(), original.len());
    assert_eq!(downloaded.as_ref(), original.as_slice());
    eprintln!("  Roundtrip:    ✓ verified");

    // Get stats
    let resp = server
        .client
        .get(format!("{}/api/stats", server.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    let stats_after_v1: serde_json::Value = resp.json().await.unwrap();

    // ── Upload tweaked version ───────────────────────────────────────────
    let t2 = Instant::now();
    let tweaked_artifacts = build_upload_artifacts(&tweaked);
    let tweak_chunk_time = t2.elapsed();

    let tweaked_xorb_bytes: usize = tweaked_artifacts
        .xorb_entries
        .iter()
        .map(|(_, b)| b.len())
        .sum();

    // Count how many xorbs are actually NEW (not already uploaded)
    let original_hashes: std::collections::HashSet<&str> = original_artifacts
        .xorb_entries
        .iter()
        .map(|(h, _)| h.as_str())
        .collect();

    let mut new_xorb_bytes = 0usize;
    let mut new_xorb_count = 0usize;
    let mut reused_xorb_count = 0usize;

    let t3 = Instant::now();
    let write_token = server.write_token();

    for (hash, data) in &tweaked_artifacts.xorb_entries {
        if original_hashes.contains(hash.as_str()) {
            reused_xorb_count += 1;
            // Upload anyway — server returns was_inserted: false
            let resp = server
                .client
                .post(format!("{}/v1/xorbs/default/{hash}", server.base_url))
                .bearer_auth(&write_token)
                .body(data.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["was_inserted"], false, "expected dedup for {hash}");
        } else {
            new_xorb_count += 1;
            new_xorb_bytes += data.len();
            let resp = server
                .client
                .post(format!("{}/v1/xorbs/default/{hash}", server.base_url))
                .bearer_auth(&write_token)
                .body(data.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["was_inserted"], true);
        }
    }

    // Upload tweaked shard
    let resp = server
        .client
        .post(format!("{}/v1/shards", server.base_url))
        .bearer_auth(&write_token)
        .body(tweaked_artifacts.shard_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let tweak_upload_time = t3.elapsed();

    eprintln!();
    eprintln!(
        "─── Tweaked upload (~5% changed, {} KB modified) ───",
        tweak_size / 1024
    );
    eprintln!(
        "  Chunks:       {}",
        tweaked_artifacts.chunk_hashes.len()
    );
    eprintln!("  Xorbs total:  {}", tweaked_artifacts.xorb_entries.len());
    eprintln!("  Xorbs reused: {} (deduped!)", reused_xorb_count);
    eprintln!("  Xorbs new:    {}", new_xorb_count);
    eprintln!(
        "  Bytes sent:   {} KB ({} bytes) ← only new data",
        new_xorb_bytes / 1024,
        new_xorb_bytes
    );
    eprintln!("  Chunk time:   {:?}", tweak_chunk_time);
    eprintln!("  Upload time:  {:?}", tweak_upload_time);

    // Verify tweaked version roundtrips correctly
    let resp = server
        .client
        .get(format!(
            "{}/api/files/{}/content",
            server.base_url, tweaked_artifacts.file_hash
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let downloaded = resp.bytes().await.unwrap();
    assert_eq!(downloaded.len(), tweaked.len());
    assert_eq!(downloaded.as_ref(), tweaked.as_slice());
    eprintln!("  Roundtrip:    ✓ verified");

    // ── Summary ──────────────────────────────────────────────────────────
    let savings_pct = if tweaked_xorb_bytes > 0 {
        100.0 - (new_xorb_bytes as f64 / tweaked_xorb_bytes as f64 * 100.0)
    } else {
        0.0
    };

    eprintln!();
    eprintln!("═══ DEDUP SAVINGS ═══");
    eprintln!(
        "  Original:       {} MB uploaded",
        original_xorb_bytes / 1_048_576
    );
    eprintln!(
        "  Tweaked (naive): {} MB would be uploaded",
        tweaked_xorb_bytes / 1_048_576
    );
    eprintln!(
        "  Tweaked (dedup): {} KB actually uploaded",
        new_xorb_bytes / 1024
    );
    eprintln!("  Savings:        {:.1}%", savings_pct);
    eprintln!(
        "  Speedup:        {:.1}x",
        original_xorb_bytes as f64 / new_xorb_bytes.max(1) as f64
    );

    // Assert meaningful dedup happened
    assert!(
        new_xorb_bytes < tweaked_xorb_bytes / 2,
        "expected >50% dedup savings, got only {:.1}%",
        savings_pct
    );
}
