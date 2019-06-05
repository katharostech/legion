use crate::{Archetype, ChunkBuilder, ComponentTypeId, Entity, EntityAllocator, TagTypeId};
use easy_ffi::*;
use fnv::FnvHashSet;
use std::os::raw::c_void;

easy_ffi!(bool_ffi =>
    |err| {
        println!("{}", err);
        false
    }
    |panic_val| {
        match panic_val.downcast_ref::<&'static str>() {
            Some(s) => println!("panic: {}", s),
            None => println!("unknown panic!"),
        }
        false
    }
);
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
pub struct EntityData {
    num_tag_types: u32,
    tag_types: *const u32,
    tag_data_sizes: *const u32,
    tag_data: *const *const c_void,
    num_component_types: u32,
    component_types: *const u32,
    component_data_sizes: *const u32,
    num_entities: u32,
    component_data: *const *const c_void,
}
#[repr(C)]
pub struct EntityResult {
    num_entities_written: u32,
    entities: *mut Entity,
}
const EXT_TYPE_ID: std::any::TypeId = std::any::TypeId::of::<ExternalComponent>();
impl crate::TagSet for &EntityData {
    fn is_archetype_match(&self, archetype: &Archetype) -> bool {
        if archetype.tags.len() != self.num_tag_types as usize {
            false
        } else {
            unsafe {
                for i in 0..self.num_tag_types {
                    if !archetype
                        .has_tag_type(&TagTypeId(EXT_TYPE_ID, *self.tag_types.offset(i as isize)))
                    {
                        return false;
                    }
                }
            }
            true
        }
    }
    fn is_chunk_match(&self, chunk: &crate::Chunk) -> bool {
        unsafe {
            for i in 0..self.num_tag_types {
                let self_data = self.tag_data.offset(i as isize) as *const u8;
                let data_size = *self.tag_data_sizes.offset(i as isize) as usize;
                let ty = TagTypeId(EXT_TYPE_ID, *self.tag_types.offset(i as isize));
                let data = chunk.tag_raw(&ty).unwrap().as_ptr();
                if std::slice::from_raw_parts(data, data_size)
                    != std::slice::from_raw_parts(self_data, data_size)
                {
                    return false;
                }
            }
            true
        }
    }
    fn configure_chunk(&self, chunk: &mut ChunkBuilder) {
        unsafe {
            for i in 0..self.num_tag_types {
                chunk.register_tag_raw(
                    TagTypeId(EXT_TYPE_ID, *self.tag_types.offset(i as isize)),
                    (*self.tag_data_sizes.offset(i as isize)) as usize,
                    crate::storage::TagStorageVTable::new(None, None, None),
                );
            }
        }
    }
    fn types(&self) -> FnvHashSet<TagTypeId> {
        let mut set = FnvHashSet::default();
        unsafe {
            for i in 0..self.num_tag_types {
                set.insert(TagTypeId(EXT_TYPE_ID, *self.tag_types.offset(i as isize)));
            }
        }
        set
    }
    fn write<'a>(&mut self, chunk: &'a mut crate::Chunk) {
        unsafe {
            for i in 0..self.num_tag_types {
                let ptr = chunk.tag_init_unchecked(&TagTypeId(
                    EXT_TYPE_ID,
                    *self.tag_types.offset(i as isize),
                ));
                std::ptr::copy_nonoverlapping(
                    (*self.tag_data.offset(i as isize)) as *mut u8,
                    ptr.unwrap().as_ptr(),
                    (*self.tag_data_sizes.offset(i as isize)) as usize,
                );
            }
        }
    }
}

impl crate::EntitySource for (&EntityData, &mut EntityResult) {
    fn is_archetype_match(&self, archetype: &crate::storage::Archetype) -> bool {
        if archetype.components.len() != self.0.num_component_types as usize {
            false
        } else {
            unsafe {
                for i in 0..self.0.num_component_types {
                    if !archetype.has_component_type(&ComponentTypeId(
                        EXT_TYPE_ID,
                        *self.0.component_types.offset(i as isize),
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
            for i in 0..self.0.num_component_types {
                chunk.register_component_raw(
                    ComponentTypeId(EXT_TYPE_ID, *self.0.component_types.offset(i as isize)),
                    (*self.0.component_data_sizes.offset(i as isize)) as usize,
                    None,
                );
            }
        }
    }
    fn types(&self) -> FnvHashSet<ComponentTypeId> {
        let mut set = FnvHashSet::default();
        unsafe {
            for i in 0..self.0.num_component_types {
                set.insert(ComponentTypeId(
                    EXT_TYPE_ID,
                    *self.0.component_types.offset(i as isize),
                ));
            }
        }
        set
    }
    fn is_empty(&mut self) -> bool {
        self.0.num_entities == self.1.num_entities_written
    }
    fn write<'a>(
        &mut self,
        chunk: &'a mut crate::storage::Chunk,
        allocator: &mut EntityAllocator,
    ) -> usize {
        let mut count = 0;
        let data = &self.0;
        let mut result = &mut self.1;

        unsafe {
            let entities = chunk.entities_unchecked();
            let src_entity_start = result.num_entities_written as usize;
            let dst_entity_start = chunk.len();
            let start = result.num_entities_written as usize;
            for _i in start..data.num_entities as usize {
                if chunk.is_full() {
                    break;
                }
                let entity = allocator.create_entity();
                entities.push(entity);
                *result.entities.offset(count as isize) = entity;
                count += 1;
            }
            for comp_idx in 0..data.num_component_types {
                let storage = chunk
                    .components_mut_unchecked_uninit_raw(
                        &ComponentTypeId(
                            EXT_TYPE_ID,
                            *data.component_types.offset(comp_idx as isize),
                        ),
                        0,
                    )
                    .expect("component storage did not exist when writing chunk");
                let comp_size = (*data.component_data_sizes.offset(comp_idx as isize)) as usize;
                std::ptr::copy_nonoverlapping(
                    (*data
                        .component_data
                        .offset((src_entity_start * comp_size) as isize))
                        as *mut u8,
                    storage
                        .as_ptr()
                        .offset((dst_entity_start * comp_size) as isize),
                    comp_size as usize * count,
                );
            }
        }
        result.num_entities_written += count as u32;

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
void_ffi!(
    fn lgn_world_free(ptr: *mut World) -> Result<(), &'static str> {
        unsafe {
            let _world = Box::from_raw(ptr as *mut crate::World);
            // let world be dropped
            Ok(())
        }
    }
);
void_ffi!(
    fn lgn_world_insert(
        ptr: *mut World,
        entity_data: *const EntityData,
        result: *mut EntityResult,
    ) -> Result<(), &'static str> {
        unsafe {
            let world = (ptr as *mut crate::World).as_mut().unwrap();
            let entity_data = entity_data.as_ref().unwrap();
            let entity_source = (entity_data, result.as_mut().unwrap());
            world.insert(entity_data, entity_source);
            Ok(())
        }
    }
);
bool_ffi!(
    fn lgn_world_delete(
        ptr: *mut World,
        entity: Entity,
    ) -> Result<bool, &'static str> {
        unsafe {
            let world = (ptr as *mut crate::World).as_mut().unwrap();
            Ok(world.delete(entity))
        }
    }
);
bool_ffi!(
    fn lgn_world_entity_is_alive(
        ptr: *mut World,
        entity: Entity,
    ) -> Result<bool, &'static str> {
        unsafe {
            let world = (ptr as *mut crate::World).as_mut().unwrap();
            Ok(world.is_alive(&entity))
        }
    }
);
ptr_ffi!(
    fn lgn_world_get_component(
        ptr: *mut World,
        ty: u32,
        entity: Entity,
    ) -> Result<*mut c_void, &'static str> {
        unsafe {
            let world = (ptr as *mut crate::World).as_mut().unwrap();
            let result = world.component_raw(&ComponentTypeId(EXT_TYPE_ID, ty), entity);
            if let Some(result) = result {
                Ok(result.as_ptr() as *mut c_void)
            } else {
                Ok(std::ptr::null_mut())
            }
        }
    }
);
ptr_ffi!(
    fn lgn_world_get_tag(
        ptr: *mut World,
        ty: u32,
        entity: Entity,
    ) -> Result<*mut c_void, &'static str> {
        unsafe {
            let world = (ptr as *mut crate::World).as_mut().unwrap();
            let result = world.tag_raw(&TagTypeId(EXT_TYPE_ID, ty), entity);
            if let Some(result) = result {
                Ok(result.as_ptr() as *mut c_void)
            } else {
                Ok(std::ptr::null_mut())
            }
        }
    }
);

#[cfg(test)]
mod tests {
    use crate::c_api::*;
    use std::ffi::c_void;

    #[derive(Clone, Copy, Debug, PartialEq)]
    struct Pos(f32, f32, f32);
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct Rot(f32, f32, f32);
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct Scale(f32, f32, f32);
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct Vel(f32, f32, f32);
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct Accel(f32, f32, f32);
    #[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
    struct Model(u32);
    #[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
    struct Static;

    #[test]
    fn create_universe() {
        let universe = lgn_universe_new();
        lgn_universe_free(universe);
    }

    #[test]
    fn create_world() {
        let universe = lgn_universe_new();
        let world = lgn_universe_create_world(universe);
        lgn_world_free(world);
        lgn_universe_free(universe);
    }
    fn type_id_as_u32<T: 'static>() -> u32 {
        use std::hash::{Hash, Hasher};
        let mut s = std::collections::hash_map::DefaultHasher::new();
        std::any::TypeId::of::<T>().hash(&mut s);
        s.finish() as u32
    }

    fn test_data() -> (Vec<Pos>, Vec<Rot>) {
        let pos_components = vec![Pos(1., 2., 3.), Pos(4., 5., 6.)];
        let rot_components = vec![Rot(0.1, 0.2, 0.3), Rot(0.4, 0.5, 0.6)];
        (pos_components, rot_components)
    }

    fn shared_data() -> (usize, f32, u16) {
        (1usize, 2f32, 3u16)
    }

    fn insert_entity(world: *mut World) -> Vec<Entity> {
        use std::mem::size_of;
        let shared = shared_data();
        let (pos_components, rot_components) = test_data();

        let tag_types: Vec<u32> = vec![
            type_id_as_u32::<usize>(),
            type_id_as_u32::<f32>(),
            type_id_as_u32::<u16>(),
        ];
        let tag_data_sizes: Vec<u32> = vec![
            size_of::<usize>() as u32,
            size_of::<f32>() as u32,
            size_of::<u16>() as u32,
        ];
        let tag_data: Vec<*const c_void> = vec![
            &shared.0 as *const usize as *const c_void,
            &shared.1 as *const f32 as *const c_void,
            &shared.2 as *const u16 as *const c_void,
        ];
        let component_types: Vec<u32> = vec![type_id_as_u32::<Pos>(), type_id_as_u32::<Rot>()];
        let component_data_sizes: Vec<u32> = vec![size_of::<Pos>() as u32, size_of::<Rot>() as u32];
        let component_data: Vec<*const c_void> = vec![
            pos_components.as_ptr() as *const c_void,
            rot_components.as_ptr() as *const c_void,
        ];
        let entity_data = EntityData {
            num_tag_types: tag_types.len() as u32,
            tag_types: tag_types.as_ptr(),
            tag_data_sizes: tag_data_sizes.as_ptr(),
            tag_data: tag_data.as_ptr(),
            num_component_types: component_types.len() as u32,
            component_types: component_types.as_ptr(),
            component_data_sizes: component_data_sizes.as_ptr(),
            component_data: component_data.as_ptr(),
            num_entities: pos_components.len() as u32,
        };
        let mut entity_result_buffer: Vec<Entity> = Vec::with_capacity(pos_components.len());
        let mut result = EntityResult {
            num_entities_written: 0,
            entities: entity_result_buffer.as_mut_ptr(),
        };
        lgn_world_insert(world, &entity_data, &mut result);
        assert!(result.num_entities_written == entity_data.num_entities);
        unsafe {
            entity_result_buffer.set_len(result.num_entities_written as usize);
        }
        entity_result_buffer
    }

    #[test]
    fn insert() {
        let universe = lgn_universe_new();
        let world = lgn_universe_create_world(universe);
        insert_entity(world);
        lgn_world_free(world);
        lgn_universe_free(universe);
    }

    #[test]
    fn get_component() {
        use std::mem::size_of;
        let universe = lgn_universe_new();
        let world = lgn_universe_create_world(universe);
        let entities = insert_entity(world);
        unsafe {
            let ptr =
                lgn_world_get_component(world, type_id_as_u32::<Pos>(), entities[1]) as *mut Pos;
            assert_ne!(std::ptr::null_mut(), ptr);
            let component = ptr.as_mut().unwrap();
            assert_eq!(component, &mut test_data().0[1]);
        }
        lgn_world_free(world);
        lgn_universe_free(universe);
    }

    #[test]
    fn get_component_wrong_type() {
        use std::mem::size_of;
        let universe = lgn_universe_new();
        let world = lgn_universe_create_world(universe);
        let entities = insert_entity(world);
        let ptr = lgn_world_get_component(world, 0, entities[1]) as *mut Pos;
        assert_eq!(std::ptr::null_mut(), ptr);
        lgn_world_free(world);
        lgn_universe_free(universe);
    }

    #[test]
    fn get_tag() {
        use std::mem::size_of;
        let universe = lgn_universe_new();
        let world = lgn_universe_create_world(universe);
        let entities = insert_entity(world);
        unsafe {
            let ptr = lgn_world_get_tag(world, type_id_as_u32::<f32>(), entities[1]) as *mut f32;
            assert_ne!(std::ptr::null_mut(), ptr);
            let tag = ptr.as_mut().unwrap();
            assert_eq!(tag, &mut shared_data().1);
        }
        lgn_world_free(world);
        lgn_universe_free(universe);
    }

    #[test]
    fn get_tag_wrong_type() {
        use std::mem::size_of;
        let universe = lgn_universe_new();
        let world = lgn_universe_create_world(universe);
        let entities = insert_entity(world);
        let ptr = lgn_world_get_tag(world, 0, entities[1]) as *mut f32;
        assert_eq!(std::ptr::null_mut(), ptr);
        lgn_world_free(world);
        lgn_universe_free(universe);
    }

    #[test]
    fn delete_entity() {
        use std::mem::size_of;
        let universe = lgn_universe_new();
        let world = lgn_universe_create_world(universe);
        let entities = insert_entity(world);
        assert_ne!(std::ptr::null_mut(), lgn_world_get_component(world, type_id_as_u32::<Pos>(), entities[1]));
        assert_eq!(lgn_world_entity_is_alive(world, entities[1]), true);
        assert_eq!(lgn_world_delete(world, entities[1]), true);
        assert_eq!(lgn_world_entity_is_alive(world, entities[1]), false);
        lgn_world_free(world);
        lgn_universe_free(universe);
    }
}
