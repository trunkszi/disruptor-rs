use core_affinity::CoreId;

pub(crate) fn cpu_has_core_else_panic(id: usize) {
	#[cfg(not(miri))]
	{
		let available: Vec<usize> = core_affinity::get_core_ids().unwrap().iter()
			.map(|core_id| core_id.id)
			.collect();

		if !available.contains(&id) {
			panic!("No core with ID={} is available.", id);
		}
	}

	#[cfg(miri)]
	{
		let _ = id;
	}
}

pub(crate) fn set_affinity_if_defined(core_affinity: Option<CoreId>, thread_name: &str) {
	#[cfg(not(miri))]
	if let Some(core_id) = core_affinity {
		let got_pinned = core_affinity::set_for_current(core_id);
		if !got_pinned {
			eprintln!("Could not pin processor thread '{}' to {:?}.", thread_name, core_id);
		}
	}

	#[cfg(miri)]
	{
		let _ = (core_affinity, thread_name);
	}
}
