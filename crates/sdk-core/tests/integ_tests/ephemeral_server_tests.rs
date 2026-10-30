use crate::common::{
    INTEG_CLIENT_IDENTITY, INTEG_CLIENT_NAME, INTEG_CLIENT_VERSION, NAMESPACE, rand_6_chars,
};
use futures_util::{TryStreamExt, stream};
use std::time::{SystemTime, UNIX_EPOCH};
use temporalio_client::{
    Connection, ConnectionOptions, WorkflowStartOptions,
    grpc::{TestService, WorkflowService},
};
use temporalio_common::protos::temporal::api::workflowservice::v1::DescribeNamespaceRequest;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    Runtime, Worker, WorkerOptions, WorkflowContext, WorkflowResult,
    testing::{LocalWorkflowEnvironmentOptions, WorkflowEnvironment},
};
use temporalio_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, EphemeralServer, TemporalDevServerConfig,
    default_cached_download,
};
use tonic::IntoRequest;
use url::Url;

#[workflow]
#[derive(Default)]
struct TestEnvironmentWorkflow;

#[workflow_methods]
impl TestEnvironmentWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
#[tokio::test]
async fn test_workflow_environment_local() {
    let env = WorkflowEnvironment::start_local(LocalWorkflowEnvironmentOptions::default())
        .await
        .unwrap();
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let worker_options = WorkerOptions::new(format!("test-env-{}", rand_6_chars()))
        .register_workflow::<TestEnvironmentWorkflow>()
        .unwrap()
        .build();
    let task_queue = worker_options.task_queue.clone();
    let mut worker = Worker::new(&runtime, env.client().clone(), worker_options).unwrap();
    let shutdown = worker.shutdown_handle();
    let handle = env
        .client()
        .start_workflow(
            TestEnvironmentWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, format!("test-env-{}", rand_6_chars())).build(),
        )
        .await
        .unwrap();

    let (worker_result, ()) = tokio::join!(worker.run(), async move {
        handle.get_result(Default::default()).await.unwrap();
        shutdown();
    });
    worker_result.unwrap();
    env.shutdown().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
#[tokio::test]
async fn temporal_cli_default() {
    let config = TemporalDevServerConfig::builder()
        .exe(default_cached_download())
        .build();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;

    // Make sure process is there on start and not there after shutdown
    let pid = sysinfo::Pid::from_u32(server.child_process_id().unwrap());
    assert!(sysinfo::System::new_all().process(pid).is_some());
    server.shutdown().await.unwrap();
    assert!(sysinfo::System::new_all().process(pid).is_none());
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
#[tokio::test]
async fn temporal_cli_fixed() {
    let config = TemporalDevServerConfig::builder()
        .exe(fixed_cached_download("v1.2.0"))
        .build();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
#[tokio::test]
async fn temporal_cli_shutdown_port_reuse() {
    // Start, test shutdown, do again immediately on same port to ensure we can
    // reuse after shutdown
    let config = TemporalDevServerConfig::builder()
        .exe(default_cached_download())
        .port(10123)
        .build();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

// This test will fail on Linux until https://github.com/temporalio/cli/pull/564
// gets released (presumably in 0.12.1). To test locally, build CLI manually
// and use that specific binary instead:
// ```
//   .exe(EphemeralExe::ExistingPath(
//       "/usr/local/bin/temporal".to_string(),
//   ))
// ```
#[tokio::test]
#[ignore]
async fn temporal_cli_concurrent_starts() -> Result<(), Box<dyn std::error::Error>> {
    stream::iter((0..80).map(|_| {
        Ok::<TemporalDevServerConfig, Box<dyn std::error::Error>>(
            TemporalDevServerConfig::builder()
                .exe(default_cached_download())
                .build(),
        )
    }))
    .try_for_each_concurrent(8, |config| async move {
        let mut server = config.start_server().await?;
        server.shutdown().await?;
        Ok(())
    })
    .await?;

    Ok(())
}

// Test server downloads aren't available for arm linux
#[cfg(not(all(target_os = "linux", any(target_arch = "arm", target_arch = "aarch64"))))]
mod test_server {
    use super::*;
    use std::{
        sync::Arc,
        time::{Instant, UNIX_EPOCH},
    };
    use temporalio_client::{
        WorkflowQueryOptions, WorkflowSignalOptions, errors::WorkflowGetResultError,
    };
    use temporalio_common::{error::IncomingError, protos::temporal::api::common::v1::RetryPolicy};
    use temporalio_macros::activities;
    use temporalio_sdk::{
        ActivityOptions, SyncWorkflowContext, TimeoutType, WorkflowContextView,
        activities::{ActivityContext, ActivityError},
        testing::{TimeSkippingServer, TimeSkippingWorkflowEnvironmentOptions},
    };
    use temporalio_sdk_core::ephemeral_server::TestServerConfig;

    #[workflow]
    #[derive(Default)]
    struct TimerWorkflow {
        last_tick_secs: u64,
    }

    #[workflow_methods]
    impl TimerWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>, timer_secs: u64) -> WorkflowResult<String> {
            ctx.timer(Duration::from_secs(timer_secs)).await;
            Ok("all done".to_owned())
        }

        /// Records workflow time, which only advances with workflow tasks, so that a following
        /// query can report it.
        #[signal]
        fn tick(&mut self, ctx: &mut SyncWorkflowContext<Self>, _: ()) {
            self.last_tick_secs = ctx
                .workflow_time()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|since_epoch| since_epoch.as_secs())
                .unwrap_or_default();
        }

        #[query]
        fn last_tick_secs(&self, _ctx: &WorkflowContextView) -> u64 {
            self.last_tick_secs
        }
    }

    struct TimeSkippingActivities {
        env: Arc<WorkflowEnvironment<TimeSkippingServer>>,
    }

    #[activities]
    impl TimeSkippingActivities {
        /// Skips twice the heartbeat timeout without heartbeating, so the attempt must time out.
        #[activity]
        async fn outlive_heartbeat_timeout(
            self: Arc<Self>,
            ctx: ActivityContext,
            _: (),
        ) -> Result<String, ActivityError> {
            let heartbeat_timeout = ctx
                .info()
                .heartbeat_timeout
                .expect("activity is scheduled with a heartbeat timeout");
            self.env
                .sleep(heartbeat_timeout * 2)
                .await
                .map_err(anyhow::Error::from)?;
            Ok("all done".to_owned())
        }
    }

    #[workflow]
    #[derive(Default)]
    struct HeartbeatTimeoutWorkflow;

    #[workflow_methods]
    impl HeartbeatTimeoutWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
            let result = ctx
                .execute_activity(
                    TimeSkippingActivities::outlive_heartbeat_timeout,
                    (),
                    ActivityOptions::with_schedule_to_close_timeout(Duration::from_secs(1000))
                        .heartbeat_timeout(Duration::from_secs(20))
                        .retry_policy(RetryPolicy {
                            maximum_attempts: 1,
                            ..Default::default()
                        })
                        .build(),
                )
                .await?;
            Ok(result)
        }
    }

    async fn start_time_skipping_env() -> WorkflowEnvironment<TimeSkippingServer> {
        WorkflowEnvironment::start_time_skipping(
            TimeSkippingWorkflowEnvironmentOptions::builder()
                .startup_timeout(Duration::from_secs(30))
                .build(),
        )
        .await
        .unwrap()
    }

    fn time_skipping_worker(client: &temporalio_client::Client, runtime: &Runtime) -> Worker {
        Worker::new(
            runtime,
            client.clone(),
            WorkerOptions::new(format!("time-skipping-{}", rand_6_chars()))
                .register_workflow::<TimerWorkflow>()
                .unwrap()
                .build(),
        )
        .unwrap()
    }

    fn secs_from_now(time: SystemTime) -> i64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64 - now
    }

    fn assert_secs_from_now(time: SystemTime, expected: i64) {
        let actual = secs_from_now(time);
        assert!(
            (actual - expected).abs() <= 30,
            "expected roughly {expected}s from now, got {actual}s"
        );
    }

    #[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
    #[tokio::test]
    async fn time_skipping_skips_timers_while_awaiting_result() {
        let env = start_time_skipping_env().await;
        let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
        let worker = time_skipping_worker(env.client(), &runtime);
        let task_queue = worker.task_queue().to_owned();
        assert_secs_from_now(env.current_time().await.unwrap(), 0);

        let handle = env
            .client()
            .start_workflow(
                TimerWorkflow::run,
                100_000,
                WorkflowStartOptions::new(task_queue, format!("timer-{}", rand_6_chars())).build(),
            )
            .await
            .unwrap();
        worker
            .run_until(async {
                // Without time-skipping this would take more than a day, so bound the wait.
                let result = tokio::time::timeout(
                    Duration::from_secs(60),
                    handle.get_result(Default::default()),
                )
                .await
                .expect("timer should be skipped well within a minute of wall time");
                assert_eq!(result.unwrap(), "all done");
            })
            .await
            .unwrap();
        assert_secs_from_now(env.current_time().await.unwrap(), 100_000);
        env.shutdown().await.unwrap();
    }

    #[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
    #[tokio::test]
    async fn time_skipping_sleep_advances_server_and_workflow_time() {
        let env = start_time_skipping_env().await;
        let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
        let worker = time_skipping_worker(env.client(), &runtime);
        let task_queue = worker.task_queue().to_owned();

        let handle = env
            .client()
            .start_workflow(
                TimerWorkflow::run,
                100_000,
                WorkflowStartOptions::new(task_queue, format!("sleep-{}", rand_6_chars())).build(),
            )
            .await
            .unwrap();
        worker
            .run_until(async {
                let workflow_now = async || {
                    handle
                        .signal(TimerWorkflow::tick, (), WorkflowSignalOptions::default())
                        .await
                        .unwrap();
                    let secs = handle
                        .query(
                            TimerWorkflow::last_tick_secs,
                            (),
                            WorkflowQueryOptions::default(),
                        )
                        .await
                        .unwrap();
                    UNIX_EPOCH + Duration::from_secs(secs)
                };
                assert_secs_from_now(workflow_now().await, 0);
                let before = env.current_time().await.unwrap();
                env.sleep(Duration::from_secs(1000)).await.unwrap();
                let after = env.current_time().await.unwrap();
                assert!(after.duration_since(before).unwrap() >= Duration::from_secs(1000));
                assert_secs_from_now(workflow_now().await, 1000);
            })
            .await
            .unwrap();
        env.shutdown().await.unwrap();
    }

    #[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
    #[tokio::test]
    async fn time_skipping_waits_for_running_activity_and_hits_heartbeat_timeout() {
        let env = Arc::new(start_time_skipping_env().await);
        let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
        let worker = Worker::new(
            &runtime,
            env.client().clone(),
            WorkerOptions::new(format!("heartbeat-{}", rand_6_chars()))
                .register_workflow::<HeartbeatTimeoutWorkflow>()
                .unwrap()
                .register_activities(TimeSkippingActivities { env: env.clone() })
                .build(),
        )
        .unwrap();
        let task_queue = worker.task_queue().to_owned();

        let handle = env
            .client()
            .start_workflow(
                HeartbeatTimeoutWorkflow::run,
                (),
                WorkflowStartOptions::new(task_queue, format!("heartbeat-{}", rand_6_chars()))
                    .build(),
            )
            .await
            .unwrap();
        worker
            .run_until(async {
                let error = handle.get_result(Default::default()).await.unwrap_err();
                let WorkflowGetResultError::Failed(failure) = error else {
                    panic!("expected workflow failure, got {error:?}");
                };
                let mut cause: Option<&IncomingError> = Some(&failure);
                let timeout = std::iter::from_fn(|| {
                    let current = cause?;
                    cause = current.cause();
                    Some(current)
                })
                .find_map(|error| error.as_timeout())
                .expect("workflow failure chain should contain a timeout");
                assert_eq!(timeout.timeout_type(), TimeoutType::Heartbeat);
            })
            .await
            .unwrap();
        Arc::into_inner(env)
            .expect("worker released its activity instances")
            .shutdown()
            .await
            .unwrap();
    }

    #[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
    #[tokio::test]
    async fn time_skipping_can_be_disabled_with_guard() {
        let env = start_time_skipping_env().await;
        let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
        let worker = time_skipping_worker(env.client(), &runtime);
        let task_queue = worker.task_queue().to_owned();
        // With skipping the timer is nearly instant; without it the whole timer elapses. The gap
        // between the thresholds leaves room for a slow CI machine on either side.
        let timer_secs = 5;
        let skipped_threshold = Duration::from_millis(2500);
        let real_time_threshold = Duration::from_secs(4);

        let run_timer_workflow = async || {
            let handle = env
                .client()
                .start_workflow(
                    TimerWorkflow::run,
                    timer_secs,
                    WorkflowStartOptions::new(
                        task_queue.clone(),
                        format!("disabled-{}", rand_6_chars()),
                    )
                    .build(),
                )
                .await
                .unwrap();
            let started = Instant::now();
            assert_eq!(
                handle.get_result(Default::default()).await.unwrap(),
                "all done"
            );
            started.elapsed()
        };
        worker
            .run_until(async {
                assert!(run_timer_workflow().await < skipped_threshold);
                {
                    let _disabled = env.auto_time_skipping_disabled();
                    assert!(run_timer_workflow().await > real_time_threshold);
                    {
                        let _nested = env.auto_time_skipping_disabled();
                    }
                    // The inner guard must not re-enable skipping that the outer guard disabled.
                    assert!(run_timer_workflow().await > real_time_threshold);
                }
                assert!(run_timer_workflow().await < skipped_threshold);
            })
            .await
            .unwrap();
        env.shutdown().await.unwrap();
    }

    #[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
    #[tokio::test]
    async fn test_server_default() {
        let config = TestServerConfig::builder()
            .exe(default_cached_download())
            .build();
        let mut server = config.start_server().await.unwrap();
        assert_ephemeral_server(&server).await;
        server.shutdown().await.unwrap();
    }

    #[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
    #[tokio::test]
    async fn test_server_fixed() {
        let config = TestServerConfig::builder()
            .exe(fixed_cached_download("v1.16.0"))
            .build();
        let mut server = config.start_server().await.unwrap();
        assert_ephemeral_server(&server).await;
        server.shutdown().await.unwrap();
    }

    #[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::RequiresLocalServer)]
    #[tokio::test]
    async fn test_server_shutdown_port_reuse() {
        // Start, test shutdown, do again immediately on same port to ensure we can
        // reuse after shutdown
        let config = TestServerConfig::builder()
            .exe(default_cached_download())
            .port(10124)
            .build();
        let mut server = config.start_server().await.unwrap();
        assert_ephemeral_server(&server).await;
        server.shutdown().await.unwrap();
        let mut server = config.start_server().await.unwrap();
        assert_ephemeral_server(&server).await;
        server.shutdown().await.unwrap();
    }
}

fn fixed_cached_download(version: &str) -> EphemeralExe {
    EphemeralExe::CachedDownload {
        version: EphemeralExeVersion::Fixed(version.to_string()),
        dest_dir: None,
        ttl: None,
    }
}

async fn assert_ephemeral_server(server: &EphemeralServer) {
    // Connect and describe namespace
    let connection_opts =
        ConnectionOptions::new(Url::try_from(&*format!("http://{}", server.target)).unwrap())
            .identity(INTEG_CLIENT_IDENTITY.to_string())
            .client_name(INTEG_CLIENT_NAME.to_string())
            .client_version(INTEG_CLIENT_VERSION.to_string())
            .build();
    let mut connection = Connection::connect(connection_opts).await.unwrap();
    let resp = connection
        .describe_namespace(
            DescribeNamespaceRequest {
                namespace: NAMESPACE.to_string(),
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .unwrap();
    assert!(resp.into_inner().namespace_info.unwrap().name == "default");

    // If it has test service, make sure we can use it too
    if server.has_test_service {
        let resp = connection
            .get_current_time(().into_request())
            .await
            .unwrap();
        // Make sure it's within 5 mins of now
        let resp_seconds = resp.get_ref().time.as_ref().unwrap().seconds as u64;
        let curr_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(curr_seconds - 300 < resp_seconds && curr_seconds + 300 > resp_seconds);
    }
}
