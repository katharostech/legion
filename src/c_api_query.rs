use crate::c_api::*;
use crate::{ComponentTypeId, TagTypeId};
use easy_ffi::*;
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
pub struct FFIFilterData {
    num_tag_types: u32,
    tag_types: *const u32,
    num_component_types: u32,
    component_types: *const u32,
    num_exclude_tag_types: u32,
    exclude_tag_types: *const u32,
    num_exclude_component_types: u32,
    exclude_component_types: *const u32,
}

#[repr(C)]
pub struct FFIAccessor {
    num_tag_types: u32,
    tag_types: *const u32,
    num_component_types: u32,
    component_types: *const u32,
}

#[repr(C)]
pub struct FFIQuery {
    world: *mut World,
    filter: FFIFilterData,
    accessor: FFIAccessor,
}

pub struct FilterData {
    tags: Vec<crate::TagTypeId>,
    components: Vec<crate::ComponentTypeId>,
    exclude_tags: Vec<crate::TagTypeId>,
    exclude_components: Vec<crate::ComponentTypeId>,
}

pub struct Accessor {
    tags: Vec<crate::TagTypeId>,
    components: Vec<crate::ComponentTypeId>,
}

pub struct Query {
    world: *mut World,
    filter: FilterData,
    accessor: Accessor,
}

struct QueryIterator {
    query: Query,
    chunks: Vec<crate::ChunkId>,
    started: bool,
    current_index: usize,
    current: FFIQueriedChunk,
    component_types: Vec<u32>,
    component_ptrs: Vec<*mut c_void>,
    tag_types: Vec<u32>,
    tag_ptrs: Vec<*const c_void>,
}

#[repr(C)]
pub struct FFIQueryIterator {
    _private: [u8; 0],
}

#[repr(C)]
pub struct FFIQueriedChunk {
    entity_count: u32,
    entities: *const crate::Entity,
    component_count: u32,
    component_types: *const u32,
    component_datas: *const *mut c_void,
    tag_count: u32,
    tag_types: *const u32,
    tag_datas: *const *const c_void,
}

impl FFIQuery {
    fn convert_query(&self) -> Result<Query, &'static str> {
        let mut filter = FilterData {
            components: Vec::new(),
            tags: Vec::new(),
            exclude_components: Vec::new(),
            exclude_tags: Vec::new(),
        };

        let mut accessor = Accessor {
            components: Vec::new(),
            tags: Vec::new(),
        };

        // Copy the received data into Rust memory.
        // Don't want to store memory pointers to external data in rust.
        unsafe {
            // Filter
            for i in 0..self.filter.num_component_types {
                let comp_type = *self.filter.component_types.offset(i as isize);
                filter
                    .components
                    .push(ComponentTypeId(ext_type_id(), comp_type));
            }
            for i in 0..self.filter.num_tag_types {
                let tag_type = *self.filter.tag_types.offset(i as isize);
                filter.tags.push(TagTypeId(ext_type_id(), tag_type));
            }
            for i in 0..self.filter.num_exclude_component_types {
                let comp_type = *self.filter.exclude_component_types.offset(i as isize);
                filter
                    .exclude_components
                    .push(ComponentTypeId(ext_type_id(), comp_type));
            }
            for i in 0..self.filter.num_exclude_tag_types {
                let tag_type = *self.filter.exclude_tag_types.offset(i as isize);
                filter.exclude_tags.push(TagTypeId(ext_type_id(), tag_type));
            }
            // Accessor
            for i in 0..self.accessor.num_component_types {
                let comp_type = *self.accessor.component_types.offset(i as isize);
                accessor
                    .components
                    .push(ComponentTypeId(ext_type_id(), comp_type));
            }
            for i in 0..self.accessor.num_tag_types {
                let tag_type = *self.accessor.tag_types.offset(i as isize);
                accessor.tags.push(TagTypeId(ext_type_id(), tag_type));
            }
        }

        // Validate that all components and tags in accessor exists in filter.
        for component in &accessor.components {
            if filter.components.contains(component) == false {
                return Err(
                    "Query Accessor attempted to access component without including it in Filter.",
                );
            }
        }
        for tag in &accessor.tags {
            if filter.tags.contains(tag) == false {
                return Err(
                    "Query Accessor attempted to access tag without including it in Filter.",
                );
            }
        }

        let result = Query {
            world: self.world,
            filter: filter,
            accessor: accessor,
        };

        Ok(result)
    }
}

impl Iterator for QueryIterator {
    type Item = *const FFIQueriedChunk;
    fn next(&mut self) -> Option<*const FFIQueriedChunk> {
        self.component_types.clear();
        self.component_ptrs.clear();
        self.tag_types.clear();
        self.tag_ptrs.clear();

        if self.started == false {
            self.current_index = 0;
            self.started = true;
        } else {
            self.current_index += 1;
        }

        if self.current_index >= self.chunks.len() {
            return None;
        }

        let world;
        unsafe {
            world = (self.query.world as *mut crate::World).as_mut().unwrap();
        }
        let chunk_id = &self
            .chunks
            .get(self.current_index)
            .expect("Invalid iterator chunk_id");
        let archetype = &world
            .archetypes
            .get(chunk_id.1 as usize)
            .expect("Iterator chunk_id pointed to invalid archetype");
        let chunk = archetype.chunk(chunk_id.2).unwrap();

        for component in &self.query.accessor.components {
            unsafe {
                let comp_ptr = chunk.components_mut_raw_untyped(&component, 0).unwrap();
                self.component_types.push(component.1);
                self.component_ptrs.push(comp_ptr.as_ptr() as *mut c_void);
            }
        }

        for tag in &self.query.accessor.tags {
            unsafe {
                let tag_ptr = chunk.tag_raw(&tag).unwrap();
                self.tag_types.push(tag.1);
                self.tag_ptrs.push(tag_ptr.as_ptr() as *const c_void);
            }
        }

        let entities_ptr = unsafe { chunk.entities().as_ptr() };

        self.current = FFIQueriedChunk {
            entity_count: chunk.len() as u32,
            entities: entities_ptr,
            component_count: self.component_types.len() as u32,
            component_types: self.component_types.as_ptr(),
            component_datas: self.component_ptrs.as_ptr(),
            tag_count: self.tag_types.len() as u32,
            tag_types: self.tag_types.as_ptr(),
            tag_datas: self.tag_ptrs.as_ptr(),
        };

        Some(&self.current as *const FFIQueriedChunk)
    }
}

fn filter_world(world: &crate::World, filter: &FilterData) -> Vec<crate::ChunkId> {
    let mut matching_chunks: Vec<crate::ChunkId> = Vec::new();
    for archetype in &world.archetypes {
        // Inclusive Tags
        if filter
            .tags
            .iter()
            .any(|x| archetype.has_tag_type(&x) == false)
        {
            continue;
        }
        // Inclusive Components
        if filter
            .components
            .iter()
            .any(|x| archetype.has_component_type(&x) == false)
        {
            continue;
        }
        // Exclusive Tags
        if filter
            .exclude_tags
            .iter()
            .any(|x| archetype.has_tag_type(&x))
        {
            continue;
        }
        // Exclusive Components
        if filter
            .exclude_components
            .iter()
            .any(|x| archetype.has_component_type(&x))
        {
            continue;
        }
        for chunk in archetype.chunks() {
            matching_chunks.push(chunk.id());
        }
    }
    matching_chunks
}

ptr_ffi!(
    fn lgn_query(ffi_query_ptr: *const FFIQuery) -> Result<*mut FFIQueryIterator, &'static str> {
        let ffi_query;
        let world;
        unsafe {
            ffi_query = (ffi_query_ptr as *const FFIQuery).as_ref().unwrap();
            world = (ffi_query.world as *mut crate::World).as_mut().unwrap();
        }

        let query: Query = ffi_query.convert_query()?;
        let matching_chunks = filter_world(&world, &query.filter);

        let result = Box::new(QueryIterator {
            query: query,
            started: false,
            current_index: 0,
            chunks: matching_chunks,
            current: FFIQueriedChunk {
                entity_count: 0,
                entities: std::ptr::null(),
                component_count: 0,
                component_types: std::ptr::null(),
                component_datas: std::ptr::null(),
                tag_count: 0,
                tag_types: std::ptr::null(),
                tag_datas: std::ptr::null(),
            },
            component_types: Vec::new(),
            component_ptrs: Vec::new(),
            tag_types: Vec::new(),
            tag_ptrs: Vec::new(),
        });
        Ok(Box::into_raw(result) as *mut FFIQueryIterator)
    }
);

ptr_ffi!(
    fn lgn_queryiterator_move_next(
        iterator_ptr: *mut FFIQueryIterator,
    ) -> Result<*const FFIQueriedChunk, &'static str> {
        let iterator = unsafe {
            (iterator_ptr as *mut QueryIterator)
                .as_mut()
                .expect("Invalid QueryIterator Pointer")
        };

        match iterator.next() {
            Some(chunk) => Ok(chunk),
            None => Ok(std::ptr::null()),
        }
    }
);

void_ffi!(
    fn lgn_query_free(iterator_ptr: *mut FFIQueryIterator) -> Result<(), &'static str> {
        unsafe {
            let _iterator = Box::from_raw(iterator_ptr as *mut QueryIterator);
            // let iterator be dropped
            Ok(())
        }
    }
);

#[cfg(test)]
mod tests {
    use crate::c_api::*;
    use crate::c_api_query::*;
    use crate::Entity;
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

    struct TestQuery {
        filter_component_types: Vec<u32>,
        filter_tag_types: Vec<u32>,
        filter_exclude_tag_types: Vec<u32>,
        filter_exclude_component_types: Vec<u32>,
        accessor_tag_types: Vec<u32>,
        accessor_component_types: Vec<u32>,
    }

    fn create_query(world: *mut World) -> (TestQuery, FFIQuery) {
        let mut test_query = TestQuery {
            filter_component_types: Vec::new(),
            filter_tag_types: Vec::new(),
            filter_exclude_tag_types: Vec::new(),
            filter_exclude_component_types: Vec::new(),
            accessor_tag_types: Vec::new(),
            accessor_component_types: Vec::new(),
        };

        let pos_type = type_id_as_u32::<Pos>();
        test_query.filter_component_types.push(pos_type);
        test_query.accessor_component_types.push(pos_type);
        println!("PosType: {}", pos_type);

        let filter = FFIFilterData {
            num_component_types: test_query.filter_component_types.len() as u32,
            component_types: test_query.filter_component_types.as_ptr(),
            num_tag_types: test_query.filter_tag_types.len() as u32,
            tag_types: test_query.filter_tag_types.as_ptr(),
            num_exclude_tag_types: test_query.filter_exclude_tag_types.len() as u32,
            exclude_tag_types: test_query.filter_exclude_tag_types.as_ptr(),
            num_exclude_component_types: test_query.filter_exclude_component_types.len() as u32,
            exclude_component_types: test_query.filter_exclude_component_types.as_ptr(),
        };

        let accessor = FFIAccessor {
            num_component_types: test_query.accessor_component_types.len() as u32,
            component_types: test_query.accessor_component_types.as_ptr(),
            num_tag_types: test_query.accessor_tag_types.len() as u32,
            tag_types: test_query.accessor_tag_types.as_ptr(),
        };

        unsafe {
            let filter_comp_type = *filter.component_types.offset(0 as isize);
            let accessor_comp_type = *accessor.component_types.offset(0 as isize);
            println!("Filter_Component: {}", filter_comp_type);
            println!("Accessor_Component: {}", accessor_comp_type);
            assert_eq!(accessor_comp_type, filter_comp_type);
        }

        let query = FFIQuery {
            world,
            filter,
            accessor,
        };

        (test_query, query)
    }

    #[test]
    fn new_query() {
        let universe = lgn_universe_new();
        let world = lgn_universe_create_world(universe);
        let _entities = insert_entity(world);

        let (_test_query, _query) = create_query(world);

        lgn_world_free(world);
        lgn_universe_free(universe);
    }

    #[test]
    fn iterate() {
        let universe = lgn_universe_new();
        let world = lgn_universe_create_world(universe);
        let _entities = insert_entity(world);

        let (_test_query, query) = create_query(world);
        let iterator = lgn_query(&query);
        assert_ne!(std::ptr::null_mut(), iterator);

        assert_ne!(std::ptr::null(), lgn_queryiterator_move_next(iterator));
        assert_eq!(std::ptr::null(), lgn_queryiterator_move_next(iterator));

        lgn_query_free(iterator);

        lgn_world_free(world);
        lgn_universe_free(universe);
    }

    #[test]
    fn free_iterator() {
        let universe = lgn_universe_new();
        let world = lgn_universe_create_world(universe);
        let _entities = insert_entity(world);

        let (_test_query, query) = create_query(world);
        let iterator = lgn_query(&query);
        assert_ne!(std::ptr::null_mut(), iterator);

        lgn_query_free(iterator);

        assert_eq!(std::ptr::null(), lgn_queryiterator_move_next(iterator));

        lgn_world_free(world);
        lgn_universe_free(universe);
    }
}
