use crate::{ChunkBuilder, ComponentTypeId, EntityAllocator};
use easy_ffi::*;
use fnv::FnvHashSet;
use std::os::raw::c_void;

easy_ffi!(ptr_ffi =>
    |err| {
        println!("{}", err);
        std::ptr::null_mut()
    }
    |panic_val| {
        match panic_val.downcast_ref::<&'static str>() {
            Some(s) => println!("panic: {}", s),
            None => println!("unknown panic!"),
        }
        std::ptr::null_mut()
    }
);
easy_ffi!(void_ffi =>
    |err| {
        println!("{}", err);
        ()
    }
    |panic_val| {
        match panic_val.downcast_ref::<&'static str>() {
            Some(s) => println!("panic: {}", s),
            None => println!("unknown panic!"),
        }
        ()
    }
);

#[repr(C)]
pub struct Universe {
    _private: [u8; 0],
}
#[repr(C)]
pub struct World {
    _private: [u8; 0],
}
/// Component that provides TypeId for external components
struct ExternalComponent;

#[repr(C)]
pub struct ExternalEntitySource {
    component_types: *const u32,
    component_data_sizes: *const u32,
    num_component_types: u32,
    num_entities: u32,
    component_data: *const *const c_void,
    num_entities_written: u32,
}
const EXT_TYPE_ID: std::any::TypeId = std::any::TypeId::of::<ExternalComponent>();

impl crate::EntitySource for ExternalEntitySource {
    fn is_archetype_match(&self, archetype: &crate::storage::Archetype) -> bool {
        if archetype.components.len() != self.num_component_types as usize {
            false
        } else {
            unsafe {
                for i in 0..self.num_component_types {
                    if !archetype.has_component_type(&ComponentTypeId(
                        EXT_TYPE_ID,
                        *self.component_types.offset(i as isize),
                    )) {
                        return false;
                    }
                }
            }
            true
        }
    }
    fn configure_chunk(&self, chunk: &mut ChunkBuilder) {
        unsafe {
            for i in 0..self.num_component_types {
                chunk.register_component_type(
                    ComponentTypeId(EXT_TYPE_ID, *self.component_types.offset(i as isize)),
                    (*self.component_data_sizes.offset(i as isize)) as usize,
                );
            }
        }
    }
    fn types(&self) -> FnvHashSet<ComponentTypeId> {
        let mut set = FnvHashSet::default();
        unsafe {
            for i in 0..self.num_component_types {
                set.insert(ComponentTypeId(
                    EXT_TYPE_ID,
                    *self.component_types.offset(i as isize),
                ));
            }
        }
        set
    }
    fn is_empty(&mut self) -> bool {
        self.num_entities == 0
    }
    fn write<'a>(
        &mut self,
        chunk: &'a mut crate::storage::Chunk,
        allocator: &mut EntityAllocator,
    ) -> usize {
        let mut count = 0;

        unsafe {
            let entities = chunk.entities_unchecked();
            for src_entity_idx in self.num_entities_written..self.num_entities {
                let src_entity_idx = src_entity_idx as usize;
                if chunk.is_full() {
                    break;
                }
                let entity = allocator.create_entity();
                entities.push(entity);
                let dst_entity_idx = chunk.len() - 1;
                for comp_idx in 0..self.num_component_types {
                    // TODO fix hash table lookup in hot path
                    let storage = chunk.components_mut_unchecked_uninit_raw(&ComponentTypeId(
                        EXT_TYPE_ID,
                        *self.component_types.offset(comp_idx as isize),
                    )).expect("component storage did not exist when writing chunk");
                    let comp_size = (*self.component_data_sizes.offset(comp_idx as isize)) as usize;
                    std::ptr::copy_nonoverlapping(
                        self.component_data.offset((src_entity_idx * comp_size) as isize) as *mut u8,
                        storage.offset((dst_entity_idx * comp_size) as isize),
                        comp_size as usize,
                    );
                    self.num_entities_written += 1;
                    count += 1;
                }
            }
        }

        count
    }
}

ptr_ffi!(
    fn lgn_universe_new() -> Result<*mut Universe, &'static str> {
        let universe = Box::new(crate::Universe::new(None));
        Ok(Box::into_raw(universe) as *mut Universe)
    }
);
void_ffi!(
    fn lgn_universe_free(ptr: *mut Universe) -> Result<(), &'static str> {
        unsafe {
            let _universe = Box::from_raw(ptr as *mut crate::Universe);
            // let universe be dropped
            Ok(())
        }
    }
);
ptr_ffi!(
    fn lgn_universe_create_world(ptr: *mut Universe) -> Result<*mut World, &'static str> {
        unsafe {
            let world = Box::new(
                (ptr as *mut crate::Universe)
                    .as_mut()
                    .expect("universe null ptr")
                    .create_world(),
            );
            Ok(Box::into_raw(world) as *mut World)
        }
    }
);
