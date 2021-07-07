// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::ffi::CString;
use std::fs::File;
use std::io::Read;
use std::sync::Once;
use std::{mem, process, ptr};

use libcgroup_sys::{
    cgroup, cgroup_free, cgroup_get_cgroup, cgroup_get_controller, cgroup_get_value_int64,
    cgroup_get_value_string, cgroup_init, cgroup_new_cgroup,
};

static CGROUP_INIT: Once = Once::new();

pub struct CGroupSys(*const cgroup);

unsafe impl Send for CGroupSys {}
unsafe impl Sync for CGroupSys {}

impl Drop for CGroupSys {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { cgroup_free(&self.0 as _) }
        }
    }
}

impl CGroupSys {
    const MEMORY: *const i8 = "memory\0".as_ptr() as _;
    const MEMORY_MAX: *const i8 = "memory.max\0".as_ptr() as _;
    const CPU: *const i8 = "cpu\0".as_ptr() as _;
    const CPU_MAX: *const i8 = "cpu.max\0".as_ptr() as _;
    const CPUSET: *const i8 = "cpuset\0".as_ptr() as _;
    const CPUSET_CPUS: *const i8 = "cpuset.cpus\0".as_ptr() as _;

    /// Create a `CGroupSys` instance representing the cgroup of the current process.
    ///
    /// The process's cgroup name is found in `/proc/<pid>/cgroup`. Here is an example
    /// for the file content: `0::/user.slice/user-1000.slice/session-27.scope`.
    ///
    /// For cgroup-v2, PTAL https://www.kernel.org/doc/html/v5.5/admin-guide/cgroup-v2.html.
    pub fn new() -> Self {
        CGROUP_INIT.call_once(|| {
            assert_eq!(unsafe { cgroup_init() }, 0, "cgroup_init fails");
        });

        let mut line = String::new();
        File::open(&format!("/proc/{}/cgroup", process::id()))
            .and_then(|mut f| f.read_to_string(&mut line))
            .expect("read /proc/<pid>/cgroup");
        let cg_path = line.trim().split(':').last().expect("parse cgroup file");
        let cg_path = CString::new(cg_path).unwrap();

        unsafe {
            let cg = cgroup_new_cgroup(cg_path.as_ptr());
            cgroup_get_cgroup(cg);
            CGroupSys(cg)
        }
    }

    /// Get CPU quota. Return `None` means no limitation.
    pub fn cpu_quota(&self) -> Option<f64> {
        let value = unsafe {
            let mut ptr: *mut i8 = ptr::null_mut();
            let controller = cgroup_get_controller(self.0, Self::CPU);
            cgroup_get_value_string(controller, Self::CPU_MAX, mem::transmute(&mut ptr));
            CString::from_raw(ptr)
        };
        parse_cpu_quota(value.to_str().unwrap())
    }

    /// Get CPU cores. An empty return value means no limitation.
    pub fn cpuset_cores(&self) -> HashSet<usize> {
        let value = unsafe {
            let mut ptr: *mut i8 = ptr::null_mut();
            let controller = cgroup_get_controller(self.0, Self::CPUSET);
            cgroup_get_value_string(controller, Self::CPUSET_CPUS, mem::transmute(&mut ptr));
            CString::from_raw(ptr)
        };
        parse_cpu_cores(value.to_str().unwrap())
    }

    pub fn memory_limit_in_bytes(&self) -> i64 {
        let controller = unsafe { cgroup_get_controller(self.0, Self::MEMORY) };
        let mut value = 0i64;
        unsafe { cgroup_get_value_int64(controller, Self::MEMORY_MAX, &mut value as _) };
        value
    }
}

fn parse_cpu_quota(value: &str) -> Option<f64> {
    let mut tuple = value.split(' ');
    let max = tuple.next().unwrap().trim();
    let period = tuple.next().unwrap().trim();
    if max != "max" {
        let max = max.parse::<u64>().unwrap() as f64;
        let period = period.parse::<u64>().unwrap() as f64;
        Some(max / period)
    } else {
        None
    }
}

fn parse_cpu_cores(value: &str) -> HashSet<usize> {
    let mut cores = HashSet::new();
    for v in value.split(',') {
        if v.contains('-') {
            let mut v = v.split('-');
            let s = v.next().unwrap().parse::<usize>().unwrap();
            let e = v.next().unwrap().parse::<usize>().unwrap();
            for x in s..=e {
                cores.insert(x);
            }
        } else {
            let s = v.parse::<usize>().unwrap();
            cores.insert(s);
        }
    }
    cores
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cgroup_sys() {
        let cg = CGroupSys::new();
        println!("memory limit: {:?}", cg.memory_limit_in_bytes());
        println!("cpu quota: {:?}", cg.cpu_quota());
        println!("cpu sets: {:?}", cg.cpuset_cores());
    }
}
