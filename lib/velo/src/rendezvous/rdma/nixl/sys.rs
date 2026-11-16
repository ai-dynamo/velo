// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`NixlAgent`] over a real NIXL agent (`nixl-sys`).
//!
//! Kept thin on purpose. CI has no libnixl, so nothing in this file runs
//! there: every rule that can be stated against the trait lives in the driver,
//! which the fake agent tests. What is here is the mapping of each trait call
//! onto NIXL calls. The facts about NIXL that the mapping depends on were
//! measured on libnixl 1.5.2 (`docs/src/development/rdma-design.md`).

use std::collections::HashMap;

use nixl_sys::{
    Agent, AgentConfig, MemType, MemoryRegion, NixlDescriptor, NixlError, OptArgs, RegDescList,
    RegistrationHandle, XferDescList, XferOp, XferRequest, XferStatus,
};

use super::NixlRdmaConfig;
use super::agent::{NixlAgent, Posted, ReadReq, XferPoll};

/// NIXL's UCX plugin.
const PLUGIN: &str = "UCX";

/// A device id no region is ever registered under. Region ids count up from
/// one, so they do not reach it.
const NEVER_REGISTERED: u64 = u64::MAX;

/// The longest sleep of NIXL's progress thread, not its latency: the thread
/// wakes on events. A delay of zero spins one core per agent (measured).
const PROGRESS_DELAY_US: u64 = 100_000;

/// Host memory, as NIXL wants a registration described.
#[derive(Debug)]
struct Host {
    ptr: usize,
    len: usize,
    dev: u64,
}

impl MemoryRegion for Host {
    unsafe fn as_ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    fn size(&self) -> usize {
        self.len
    }
}

impl NixlDescriptor for Host {
    fn mem_type(&self) -> MemType {
        MemType::Dram
    }

    fn device_id(&self) -> u64 {
        self.dev
    }
}

fn failed(call: &'static str) -> impl Fn(NixlError) -> String {
    move |e| format!("nixl {call}: {e}")
}

pub(crate) struct SysAgent {
    agent: Agent,
    /// Names the UCX backend in registration and metadata calls.
    backend_opt: OptArgs,
    /// One per worker, each with `worker_id=N`. A READ is pinned to a worker,
    /// because NIXL otherwise gives every READ from one thread to one worker.
    worker_opts: Vec<OptArgs>,
    /// The handle of each registration, by device id. Dropping a handle
    /// deregisters, so they are kept until `deregister` asks.
    handles: HashMap<u64, RegistrationHandle>,
}

impl SysAgent {
    /// Create the agent and its UCX backend. Call it on the thread that will
    /// make every later call.
    pub(crate) fn create(name: &str, cfg: &NixlRdmaConfig) -> Result<Self, String> {
        let workers = cfg.num_workers.max(1);
        let agent_cfg = AgentConfig {
            // The progress thread stays on. Without a native RMA lane the
            // owner must progress for a READ to finish, and an owner that only
            // serves reads makes no NIXL call of its own.
            enable_prog_thread: true,
            enable_listen_thread: false,
            pthr_delay_us: PROGRESS_DELAY_US,
            ..Default::default()
        };
        let agent = Agent::new_configured(name, &agent_cfg).map_err(failed("create_agent"))?;

        let (_mems, mut params) = agent
            .get_plugin_params(PLUGIN)
            .map_err(failed("get_plugin_params"))?;
        let set = |params: &mut nixl_sys::Params, key: &str, value: &str| {
            params
                .set(key, value)
                .map_err(|e| format!("nixl params.set {key}={value}: {e}"))
        };
        set(&mut params, "num_workers", &workers.to_string())?;
        if !cfg.devices.is_empty() {
            set(&mut params, "ucx_devices", &cfg.devices)?;
        }
        // The UCX defaults of velo's UCX transport: no registration cache and
        // no memory hooks, so that a deregistration unpins. NIXL 1.5.2 accepts
        // this parameter, and it accepts a key UCX does not have, so nothing
        // shows that it takes effect. Set `UCX_RCACHE_ENABLE=n` and
        // `UCX_MEM_EVENTS=n` in the environment to be sure.
        set(&mut params, "engine_config", "RCACHE_ENABLE=n,MEM_EVENTS=n")?;

        let backend = agent
            .create_backend(PLUGIN, &params)
            .map_err(failed("create_backend"))?;
        let mut backend_opt = OptArgs::new().map_err(failed("opt_args"))?;
        backend_opt
            .add_backend(&backend)
            .map_err(failed("opt_args.add_backend"))?;

        let mut worker_opts = Vec::with_capacity(workers as usize);
        for worker in 0..workers {
            let mut opt = OptArgs::new().map_err(failed("opt_args"))?;
            opt.set_custom_param(format!("worker_id={worker}").as_bytes())
                .map_err(failed("opt_args.set_custom_param"))?;
            worker_opts.push(opt);
        }

        Ok(Self {
            agent,
            backend_opt,
            worker_opts,
            handles: HashMap::new(),
        })
    }

    /// NIXL's metadata for one region.
    ///
    /// For a region the agent does not hold, the answer depends on what else
    /// the agent holds (measured): `NotFound` if it holds any other region,
    /// and a blob with no region in it if it holds none.
    fn region_md(&self, addr: usize, len: usize, dev: u64) -> Result<Vec<u8>, NixlError> {
        let mut list = RegDescList::new(MemType::Dram)?;
        list.add_desc(addr, len, dev);
        self.agent
            .get_local_partial_md(&list, Some(&self.backend_opt))
    }
}

impl NixlAgent for SysAgent {
    type Xfer = XferRequest;

    fn conn_blob(&mut self) -> Result<Vec<u8>, String> {
        // No descriptor: connection info only.
        let empty = RegDescList::new(MemType::Dram).map_err(failed("reg_dlist"))?;
        self.agent
            .get_local_partial_md(&empty, Some(&self.backend_opt))
            .map_err(failed("get_local_partial_md"))
    }

    fn register(&mut self, addr: usize, len: usize, dev: u64) -> Result<Vec<u8>, String> {
        let handle = self
            .agent
            .register_memory(
                &Host {
                    ptr: addr,
                    len,
                    dev,
                },
                Some(&self.backend_opt),
            )
            .map_err(failed("register_mem"))?;
        // If this fails, `handle` drops and the region is deregistered.
        let md = self
            .region_md(addr, len, dev)
            .map_err(failed("get_local_partial_md"))?;
        self.handles.insert(dev, handle);
        Ok(md)
    }

    fn deregister(&mut self, addr: usize, len: usize, dev: u64) -> Result<(), String> {
        if let Some(mut handle) = self.handles.remove(&dev) {
            // Always `Ok`: the bindings drop NIXL's status.
            handle.deregister().map_err(failed("deregister_mem"))?;
        }
        // So ask NIXL what it still knows. A region that is gone answers as a
        // region that never existed does, whichever of the two answers that
        // is. A region that is still held answers with its metadata.
        let now = self.region_md(addr, len, dev);
        let never = self.region_md(addr, len, NEVER_REGISTERED);
        match (now, never) {
            (Err(NixlError::NotFound), Err(NixlError::NotFound)) => Ok(()),
            (Ok(now), Ok(never)) if now == never => Ok(()),
            (Ok(_), _) => {
                Err("nixl deregister_mem: the agent still has metadata for the region".to_string())
            }
            (Err(e), _) => Err(failed("get_local_partial_md")(e)),
        }
    }

    fn load_remote(&mut self, blob: &[u8]) -> Result<String, String> {
        self.agent
            .load_remote_md(blob)
            .map_err(failed("load_remote_md"))
    }

    fn invalidate_remote(&mut self, agent: &str) -> Result<(), String> {
        self.agent
            .invalidate_remote_md(agent)
            .map_err(failed("invalidate_remote_md"))
    }

    fn post_read(&mut self, req: &ReadReq<'_>) -> Result<Posted<XferRequest>, String> {
        let remote_addr = usize::try_from(req.remote_addr)
            .map_err(|_| "nixl create_xfer_req: remote address out of range".to_string())?;
        let mut local = XferDescList::new(MemType::Dram).map_err(failed("xfer_dlist"))?;
        local.add_desc(req.local_addr, req.len, req.local_dev);
        let mut remote = XferDescList::new(MemType::Dram).map_err(failed("xfer_dlist"))?;
        remote.add_desc(remote_addr, req.len, req.remote_dev);
        let opt = &self.worker_opts[req.worker % self.worker_opts.len()];
        let xfer = self
            .agent
            .create_xfer_req(XferOp::Read, &local, &remote, req.peer, Some(opt))
            .map_err(failed("create_xfer_req"))?;
        // A post that fails did not start the transfer, so dropping `xfer`
        // (which releases it) is safe on that path.
        match self
            .agent
            .post_xfer_req(&xfer, None)
            .map_err(failed("post_xfer_req"))?
        {
            true => Ok(Posted::Pending(xfer)),
            false => Ok(Posted::Done),
        }
    }

    fn poll(&mut self, xfer: &mut XferRequest) -> XferPoll {
        match self.agent.get_xfer_status(xfer) {
            Ok(XferStatus::Success) => XferPoll::Done,
            Ok(XferStatus::InProgress) => XferPoll::InProgress,
            // NIXL's state for the peer is gone. It did not ask the backend,
            // so this says nothing about the transfer.
            Err(NixlError::NotFound) => {
                XferPoll::Unconfirmed("the peer's state was dropped during the transfer".into())
            }
            Err(e) => XferPoll::Failed(e.to_string()),
        }
    }

    fn finish(&mut self, xfer: XferRequest) {
        drop(xfer);
    }
}
