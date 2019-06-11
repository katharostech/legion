use crate::*;
use downcast_rs::{impl_downcast, Downcast};
use fnv::{FnvHashMap, FnvHashSet};
use std::any::TypeId;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::mem::size_of;
use std::sync::atomic::AtomicIsize;
use std::sync::Arc;

impl_downcast!(ComponentStorage);
trait ComponentStorage: Downcast + Debug {
    fn remove(&mut self, id: ComponentIndex);
    fn fetch_remove(
        &mut self,
        id: ComponentIndex,
    ) -> (
        ComponentTypeId,
        Box<dyn Fn(&mut ChunkBuilder)>,
        Box<dyn ChunkInit>,
    );
    fn len(&self) -> usize;
}

#[derive(Debug)]
struct StorageVec<T: Component> {
    version: UnsafeCell<Wrapping<usize>>,
    data: UnsafeCell<Vec<T>>,
}

impl<T: Component> StorageVec<T> {
    fn with_capacity(capacity: usize) -> Self {
        StorageVec {
            version: UnsafeCell::new(Wrapping(0)),
            data: UnsafeCell::new(Vec::<T>::with_capacity(capacity)),
        }
    }

    fn version(&self) -> usize {
        unsafe { (*self.version.get()).0 }
    }

    unsafe fn data(&self) -> &Vec<T> {
        &(*self.data.get())
    }

    unsafe fn data_mut(&self) -> &mut Vec<T> {
        *self.version.get() += Wrapping(1);
        &mut (*self.data.get())
    }
}

impl<T: Component> ComponentStorage for StorageVec<T> {
    fn remove(&mut self, id: ComponentIndex) {
        unsafe {
            self.data_mut().swap_remove(id as usize);
        }
    }

    fn fetch_remove(
        &mut self,
        id: ComponentIndex,
    ) -> (
        ComponentTypeId,
        Box<dyn Fn(&mut ChunkBuilder)>,
        Box<dyn ChunkInit>,
    ) {
        let component = unsafe { self.data_mut().swap_remove(id as usize) };
        DynamicSingleEntitySource::component_tuple(component)
    }

    fn len(&self) -> usize {
        unsafe { self.data().len() }
    }
}

impl_downcast!(TagValue);
pub trait TagValue: Downcast + Sync + Send + Debug + 'static {
    fn downcast_equals(&self, _: &TagValue) -> bool;
}

const COMPONENT_STORAGE_ALIGNMENT: usize = 16;

#[derive(Debug)]
struct ComponentStorageHeader {
    version: Wrapping<usize>,
}

impl ComponentStorageHeader {
    pub fn version(&self) -> usize {
        self.version.0
    }
}

#[derive(Clone, Copy)]
struct ComponentStorageInfo {
    ptr: *mut u8,
    component_size: usize,
    drop_fn: Option<fn(*mut u8)>,
}
impl std::fmt::Debug for ComponentStorageInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ComponentStorageInfo")
            .field("ptr", &self.ptr)
            .field("component_size", &self.component_size)
            .finish()
    }
}

impl ComponentStorageInfo {
    pub unsafe fn header(&self) -> &mut ComponentStorageHeader {
        (align_down(
            self.ptr as usize - std::mem::size_of::<ComponentStorageHeader>(),
            COMPONENT_STORAGE_ALIGNMENT,
        ) as *mut ComponentStorageHeader)
            .as_mut()
            .unwrap()
    }
    pub unsafe fn element(&self, idx: usize) -> *const u8 {
        self.ptr.offset((idx * self.component_size) as isize)
    }
    pub unsafe fn element_mut(&self, idx: usize) -> *mut u8 {
        self.header().version += Wrapping(1);
        self.ptr.offset((idx * self.component_size) as isize)
    }
    pub unsafe fn data(&self) -> *const u8 {
        self.ptr
    }
    pub unsafe fn data_mut(&self) -> *mut u8 {
        self.header().version += Wrapping(1);
        self.ptr
    }
}
/// Raw unsafe storage for components associated with entities.
///
/// All entities contained within a chunk have the same shared data values and entity data types.
///
/// Data slices obtained from a chunk when indexed with a given index all refer to the same entity.
#[derive(Debug)]
pub struct Chunk {
    id: ChunkId,
    capacity: usize,
    entities: StorageVec<Entity>,
    component_data_info_by_type: FnvHashMap<ComponentTypeId, ComponentStorageInfo>,
    component_data_info: Vec<(ComponentTypeId, ComponentStorageInfo)>,
    tags: FnvHashMap<ComponentTypeId, Arc<dyn TagValue>>,
    borrows: FnvHashMap<ComponentTypeId, AtomicIsize>,
    component_data_layout: std::alloc::Layout,
    component_data: *mut u8,
}

impl Drop for Chunk {
    fn drop(&mut self) {
        unsafe {
            // drop all components
            for (_, storage) in self.component_data_info.iter() {
                if let Some(drop_fn) = storage.drop_fn {
                    let data = storage.data_mut();
                    for i in 0..self.len() {
                        drop_fn(data.offset((i * storage.component_size) as isize));
                    }
                }
            }
            std::alloc::dealloc(self.component_data, self.component_data_layout);
        }
    }
}

unsafe impl Sync for Chunk {}

impl Chunk {
    /// Gets the ID of the chunk.
    pub fn id(&self) -> ChunkId {
        self.id
    }

    /// Gets the number of entities stored within the chunk.
    pub fn len(&self) -> usize {
        self.entities.len()
    }

    /// Determines if the chunk has reached capacity and can no longer accept more entities.
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity
    }

    /// Gets a slice of `Entity` IDs of entities contained within the chunk.
    ///
    /// # Safety
    ///
    /// This function bypasses any borrow checking. Ensure no other code is writing into
    /// the chunk's entities vector before calling this function.
    pub unsafe fn entities(&self) -> &[Entity] {
        self.entities.data()
    }

    /// Gets a mutable vector of entity IDs contained within the chunk.
    ///
    /// # Safety
    ///
    /// This function bypasses any borrow checking. Ensure no other code is reading or writing
    /// the chunk's entities vector before calling this function.
    pub unsafe fn entities_unchecked(&self) -> &mut Vec<Entity> {
        self.entities.data_mut()
    }

    /// Gets a vector of component data.
    ///
    /// Returns `None` if the chunk does not contain the requested data type.
    ///
    /// # Safety
    ///
    /// This function bypasses any borrow checking. Ensure no other code is writing to
    /// this component type in the chunk before calling this function.
    pub unsafe fn components_unchecked<T: Component>(&self) -> Option<&[T]> {
        self.component_data_info_by_type
            .get(&ComponentTypeId(TypeId::of::<T>(), 0))
            .map(|c| std::slice::from_raw_parts(c.data() as *const T, self.len()))
    }

    /// Gets a mutable vector of component data.
    ///
    /// Returns `None` if the chunk does not contain the requested data type.
    ///
    /// # Safety
    ///
    /// This function bypasses any borrow checking. Ensure no other code is reading or writing to
    /// this component type in the chunk before calling this function.
    pub unsafe fn components_mut_unchecked<T: Component>(&self) -> Option<&mut [T]> {
        self.component_data_info_by_type
            .get(&ComponentTypeId(TypeId::of::<T>(), 0))
            .map(|c| std::slice::from_raw_parts_mut(c.data_mut() as *mut T, self.len()))
    }

    /// Gets a mutable vector of all, possibly uninitialized, component data.
    ///
    /// Returns `None` if the chunk does not contain the requested data type.
    ///
    /// # Safety
    ///
    /// This function bypasses any borrow checking. Ensure no other code is reading or writing to
    /// this component type in the chunk before calling this function.
    ///
    /// This function ignores the number of entities allocated for the chunk and may return
    /// uninitialized data.
    pub unsafe fn components_mut_raw<T: Component>(
        &self,
    ) -> Option<*mut T> {
        self.component_data_info_by_type
            .get(&ComponentTypeId(TypeId::of::<T>(), 0))
            .map(|c| 
                    c.data_mut() as *mut T)
    }

    pub unsafe fn components_mut_raw_untyped(
        &self,
        ty: &ComponentTypeId,
    ) -> Option<*mut u8> {
        self.component_data_info_by_type
            .get(ty)
            .map(|c| c.data_mut())
    }

    /// Gets a slice of component data.
    ///
    /// Returns `None` if the chunk does not contain the requested data type.
    ///
    /// # Panics
    ///
    /// This function performs runtime borrow checking. It will panic if other code is borrowing
    /// the same component type mutably.
    pub fn components<'a, T: Component>(&'a self) -> Option<BorrowedSlice<'a, T>> {
        match unsafe { self.components_unchecked() } {
            Some(data) => {
                let borrow = self.borrow::<T>();
                Some(BorrowedSlice::new(data, borrow))
            }
            None => None,
        }
    }

    /// Gets a mutable slice of component data.
    ///
    /// Returns `None` if the chunk does not contain the requested data type.
    ///
    /// # Panics
    ///
    /// This function performs runtime borrow checking. It will panic if other code is borrowing
    /// the same component type.
    pub fn components_mut<'a, T: Component>(&'a self) -> Option<BorrowedMutSlice<'a, T>> {
        match unsafe { self.components_mut_unchecked() } {
            Some(data) => {
                let borrow = self.borrow_mut::<T>();
                Some(BorrowedMutSlice::new(data, borrow))
            }
            None => None,
        }
    }

    unsafe fn component_storage_header<T: Component>(&self) -> Option<&mut ComponentStorageHeader> {
        self.component_data_info_by_type
            .get(&ComponentTypeId(TypeId::of::<T>(), 0))
            .map(|c| c.header())
    }

    /// Gets the version number of a given component type.
    ///
    /// Each component array in the slice has a version number which is
    /// automatically incremented every time it is retrieved mutably.
    pub fn component_version<T: Component>(&self) -> Option<usize> {
        unsafe { self.component_storage_header::<T>().map(|s| s.version()) }
    }

    /// Gets a tag value associated with all entities in the chunk.
    ///
    /// Returns `None` if the chunk does not contain the requested data type.
    pub fn tag<T: Tag>(&self) -> Option<&T> {
        self.tags
            .get(&ComponentTypeId(TypeId::of::<T>(), 0))
            .and_then(|s| s.downcast_ref::<T>())
    }

    /// Removes an entity from the chunk.
    ///
    /// Returns the ID of any entity which was swapped into the location of the
    /// removed entity.
    pub fn remove(&mut self, id: ComponentIndex) -> Option<Entity> {
        unsafe {
            let index = id as usize;
            self.entities.data_mut().swap_remove(index);
            for (_, storage) in self.component_data_info.iter() {
                let to_remove = storage.element_mut(id as usize);
                // Drop component
                if let Some(drop_fn) = storage.drop_fn {
                    drop_fn(to_remove);
                }
                // Move the last element into the place of the dropped component
                let swap_target = storage.element_mut(self.entities().len());
                std::ptr::copy_nonoverlapping(swap_target, to_remove, storage.component_size);
            }

            if self.entities.len() > index {
                Some(*self.entities.data().get(index).unwrap())
            } else {
                None
            }
        }
    }

    /// Removes and entity from the chunk and returns a dynamic tag set and entity source
    /// which can be used to re-insert the removed entity into a world.
    ///
    /// Returns the ID of any entity which was swapped into the location of the
    /// removed entity.
    pub fn fetch_remove(
        &mut self,
        id: ComponentIndex,
    ) -> (Option<Entity>, DynamicTagSet, DynamicSingleEntitySource) {
        unsafe {
            let index = id as usize;
            let entity = self.entities.data_mut().swap_remove(index);
            // TODO ensure the returned structs drop the owned component data when they are dropped
            let components = self.component_data_info.iter().map(|(ty, storage)| {
                DynamicSingleEntitySource::component_tuple_raw(
                    *ty,
                    Vec::from(std::slice::from_raw_parts(
                        storage.element(index),
                        storage.component_size,
                    )),
                    storage.drop_fn,
                )
            });
            let tags = DynamicTagSet {
                tags: self.tags.clone(),
            };

            let components = DynamicSingleEntitySource {
                entity,
                components: components.collect(),
            };

            let moved = if self.entities.len() > index {
                Some(*self.entities.data().get(index).unwrap())
            } else {
                None
            };

            (moved, tags, components)
        }
    }

    fn borrow<'a, T: Component>(&'a self) -> Borrow<'a> {
        let id = ComponentTypeId(TypeId::of::<T>(), 0);
        let state = self
            .borrows
            .get(&id)
            .expect("entity data type not found in chunk");
        Borrow::aquire_read(state).unwrap()
    }

    fn borrow_mut<'a, T: Component>(&'a self) -> Borrow<'a> {
        let id = ComponentTypeId(TypeId::of::<T>(), 0);
        let state = self
            .borrows
            .get(&id)
            .expect("entity data type not found in chunk");
        Borrow::aquire_write(state).unwrap()
    }
}
fn align_down(addr: usize, align: usize) -> usize {
    addr & align.wrapping_neg()
}

fn align_up(addr: usize, align: usize) -> usize {
    (addr + (align - 1)) & align.wrapping_neg()
}

/// Constructs a new `Chunk`.
pub struct ChunkBuilder {
    components: Vec<(ComponentTypeId, usize, Option<fn(*mut u8)>)>,
    tags: FnvHashMap<ComponentTypeId, Arc<dyn TagValue>>,
}

impl ChunkBuilder {
    const MAX_SIZE: usize = 16 * 1024;

    /// Constructs a new `ChunkBuilder`.
    pub fn new() -> ChunkBuilder {
        ChunkBuilder {
            components: Vec::new(),
            tags: FnvHashMap::default(),
        }
    }

    /// Registers an entity data component type.
    pub fn register_component<T: Component>(&mut self) {
        self.register_component_type(
            ComponentTypeId(TypeId::of::<T>(), 0),
            size_of::<T>(),
            // None,
            Some(|ptr| unsafe { std::ptr::drop_in_place::<T>(ptr as *mut T) }),
        );
    }
    pub fn register_component_type(
        &mut self,
        id: ComponentTypeId,
        component_size: usize,
        drop_fn: Option<fn(*mut u8)>,
    ) {
        self.components.push((id, component_size, drop_fn));
    }

    /// Registers a tag type.
    pub fn register_tag<T: Tag>(&mut self, data: Arc<T>) {
        self.tags.insert(
            ComponentTypeId(TypeId::of::<T>(), 0),
            data as Arc<dyn TagValue>,
        );
    }

    pub fn register_tags<I>(&mut self, tags: I)
    where
        I: IntoIterator<Item = (ComponentTypeId, Arc<dyn TagValue>)>,
    {
        self.tags.extend(tags);
    }

    /// Builds a new `Chunk`.
    pub fn build(self, id: ChunkId) -> Chunk {
        let size_per_entity = self
            .components
            .iter()
            .map(|(_, size, _)| size)
            .sum::<usize>()
            + std::mem::size_of::<Entity>();
        let entity_capacity = std::cmp::max(1, ChunkBuilder::MAX_SIZE / size_per_entity);
        let mut data_capacity = 0usize;
        let alignment = COMPONENT_STORAGE_ALIGNMENT;
        let mut component_data_offsets = Vec::new();
        for (ty, component_size, drop_fn) in self.components.iter() {
            // First, align storage segment header
            data_capacity = align_up(data_capacity, alignment);
            // Storage segment header is stored before data
            data_capacity += std::mem::size_of::<ComponentStorageHeader>();
            // Component data is aligned after ComponentStorageHeader
            data_capacity = align_up(data_capacity, alignment);
            // Stored pointer points directly to storage data, *not ComponentStorageHeader*
            component_data_offsets.push((*ty, data_capacity, *component_size, drop_fn));
            data_capacity += component_size * entity_capacity;
        }
        let data_layout = std::alloc::Layout::from_size_align(data_capacity, alignment)
            .expect("invalid component data size/alignment");

        unsafe {
            let data_storage = std::alloc::alloc(data_layout);
            let storage_info: Vec<_> = component_data_offsets
                .into_iter()
                .map(|(ty, offset, size, drop_fn)| {
                    (
                        ty,
                        ComponentStorageInfo {
                            ptr: data_storage.offset(offset as isize),
                            component_size: size,
                            drop_fn: *drop_fn,
                        },
                    )
                })
                .collect();
            // initialize headers
            for (_, info) in storage_info.iter() {
                *info.header() = ComponentStorageHeader {
                    version: Wrapping(0),
                };
            }
            Chunk {
                id,
                capacity: entity_capacity,
                borrows: self
                    .components
                    .iter()
                    .map(|(id, _, _)| (*id, AtomicIsize::new(0)))
                    .collect(),
                entities: StorageVec::with_capacity(entity_capacity),
                component_data_info_by_type: storage_info
                    .iter()
                    .map(|(ty, info)| (*ty, *info))
                    .collect(),
                component_data_info: storage_info,
                tags: self.tags,
                component_data_layout: data_layout,
                component_data: data_storage,
            }
        }
    }
}

pub struct DynamicTagSet {
    tags: FnvHashMap<ComponentTypeId, Arc<dyn TagValue>>,
}

impl DynamicTagSet {
    pub fn set_tag<T: Tag>(&mut self, tag: Arc<T>) {
        self.tags.insert(ComponentTypeId(TypeId::of::<T>(), 0), tag);
    }

    pub fn remove_tag<T: Tag>(&mut self) -> bool {
        self.tags
            .remove(&ComponentTypeId(TypeId::of::<T>(), 0))
            .is_some()
    }
}

impl TagSet for DynamicTagSet {
    fn is_archetype_match(&self, archetype: &Archetype) -> bool {
        archetype.tags.len() == self.tags.len()
            && self.tags.keys().all(|k| archetype.tags.contains(k))
    }

    fn is_chunk_match(&self, chunk: &Chunk) -> bool {
        self.tags
            .iter()
            .all(|(k, v)| chunk.tags.get(k).unwrap().downcast_equals(v.as_ref()))
    }

    fn configure_chunk(&self, chunk: &mut ChunkBuilder) {
        chunk.register_tags(self.tags.iter().map(|(k, v)| (*k, v.clone())));
    }

    fn types(&self) -> FnvHashSet<ComponentTypeId> {
        self.tags.keys().map(|id| *id).collect()
    }
}

trait ChunkInit: Send {
    fn call(self: Box<Self>, chunk: &mut Chunk, idx: usize);
}

impl<'a, F: FnOnce(&mut Chunk, usize) + Send> ChunkInit for F {
    fn call(self: Box<Self>, chunk: &mut Chunk, idx: usize) {
        (*self)(chunk, idx);
    }
}

pub struct DynamicSingleEntitySource {
    entity: Entity,
    components: Vec<(
        ComponentTypeId,
        Box<dyn Fn(&mut ChunkBuilder)>,
        Box<dyn ChunkInit>,
    )>,
}

impl DynamicSingleEntitySource {
    fn component_tuple<T: Component>(
        component: T,
    ) -> (
        ComponentTypeId,
        Box<dyn Fn(&mut ChunkBuilder)>,
        Box<dyn ChunkInit>,
    ) {
        let chunk_setup = |chunk: &mut ChunkBuilder| chunk.register_component::<T>();

        let ty = ComponentTypeId(TypeId::of::<T>(), 0);
        let data_initializer = |chunk: &mut Chunk, idx: usize| unsafe {
            std::ptr::write(chunk.components_mut_raw::<T>().unwrap().offset(idx as isize), component);
        };

        (ty, Box::new(chunk_setup), Box::new(data_initializer))
    }

    unsafe fn component_tuple_raw(
        ty: ComponentTypeId,
        component: Vec<u8>,
        drop_fn: Option<fn(*mut u8)>,
    ) -> (
        ComponentTypeId,
        Box<dyn Fn(&mut ChunkBuilder)>,
        Box<dyn ChunkInit>,
    ) {
        let comp_size = component.len();
        let chunk_setup =
            move |chunk: &mut ChunkBuilder| chunk.register_component_type(ty, comp_size, drop_fn);

        let data_initializer = move |chunk: &mut Chunk, idx: usize| {
            std::ptr::copy_nonoverlapping(
                component.get_unchecked(0),
                chunk
                    .components_mut_raw_untyped(&ty)
                    .unwrap()
                    .offset((idx * component.len()) as isize),
                component.len(),
            );
        };

        (ty, Box::new(chunk_setup), Box::new(data_initializer))
    }

    pub fn add_component<T: Component>(&mut self, component: T) {
        self.remove_component::<T>();
        self.components.push(Self::component_tuple(component));
    }

    pub fn remove_component<T: Component>(&mut self) -> bool {
        let type_id = ComponentTypeId(TypeId::of::<T>(), 0);
        if let Some(i) = self
            .components
            .iter()
            .enumerate()
            .filter(|(_, (id, _, _))| id == &type_id)
            .map(|(i, _)| i)
            .next()
        {
            self.components.remove(i);
            true
        } else {
            false
        }
    }
}

impl EntitySource for DynamicSingleEntitySource {
    fn is_archetype_match(&self, archetype: &Archetype) -> bool {
        archetype.components.len() == self.components.len()
            && self
                .components
                .iter()
                .all(|(id, _, _)| archetype.components.contains(&id))
    }

    fn configure_chunk(&self, chunk: &mut ChunkBuilder) {
        for (_, f, _) in self.components.iter() {
            f(chunk);
        }
    }

    fn types(&self) -> FnvHashSet<ComponentTypeId> {
        self.components.iter().map(|(id, _, _)| *id).collect()
    }

    fn is_empty(&mut self) -> bool {
        self.components.len() == 0
    }

    fn write<'a>(&mut self, chunk: &'a mut Chunk, _: &mut EntityAllocator) -> usize {
        if !chunk.is_full() {
            unsafe {
                chunk.entities_unchecked().push(self.entity);
                let idx = chunk.len() - 1;
                for (_, _, f) in self.components.drain(..) {
                    f.call(chunk, idx);
                }
            }

            1
        } else {
            0
        }
    }
}

/// Stores all chunks with a given data layout.
pub struct Archetype {
    id: ArchetypeId,
    logger: slog::Logger,
    next_chunk_id: u16,
    version: u16,
    /// The entity data component types that all chunks contain.
    pub components: FnvHashSet<ComponentTypeId>,
    /// The tag types that all chunks contains.
    pub tags: FnvHashSet<ComponentTypeId>,
    /// The chunks that belong to this archetype.
    pub chunks: Vec<Chunk>,
}

impl Archetype {
    /// Constructs a new `Archetype`.
    pub fn new(
        id: ArchetypeId,
        logger: slog::Logger,
        components: FnvHashSet<ComponentTypeId>,
        tags: FnvHashSet<ComponentTypeId>,
    ) -> Archetype {
        Archetype {
            id,
            logger,
            next_chunk_id: 0,
            version: 0,
            components,
            tags,
            chunks: Vec::new(),
        }
    }

    /// Gets the archetype ID.
    pub fn id(&self) -> ArchetypeId {
        self.id
    }

    /// Gets the archetype version.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Gets a chunk reference.
    pub fn chunk(&self, id: ChunkIndex) -> Option<&Chunk> {
        self.chunks.get(id as usize)
    }

    /// Gets a mutable chunk reference.
    pub fn chunk_mut(&mut self, id: ChunkIndex) -> Option<&mut Chunk> {
        self.chunks.get_mut(id as usize)
    }

    /// Determines if the archetype's chunks contain the given entity data component type.
    pub fn has_component<T: Component>(&self) -> bool {
        self.has_component_type(&ComponentTypeId(TypeId::of::<T>(), 0))
    }

    /// Determines if the archetype's chunks contain the given entity data component type id.
    pub fn has_component_type(&self, ty: &ComponentTypeId) -> bool {
        self.components.contains(ty)
    }

    /// Determines if the archetype's chunks contain the given tag type.
    pub fn has_tag<T: Tag>(&self) -> bool {
        self.tags.contains(&ComponentTypeId(TypeId::of::<T>(), 0))
    }

    /// Gets a slice reference of chunks.
    pub fn chunks(&self) -> &[Chunk] {
        &self.chunks
    }

    /// Finds a chunk which is suitable for the given data sources, or constructs a new one.
    pub fn get_or_create_chunk<'a, 'b, 'c, S: TagSet, C: EntitySource>(
        &'a mut self,
        tags: &'b S,
        components: &'c C,
    ) -> (ChunkIndex, &'a mut Chunk) {
        match self
            .chunks
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.is_full() && tags.is_chunk_match(c))
            .map(|(i, _)| i)
            .next()
        {
            Some(i) => (i as ChunkIndex, unsafe { self.chunks.get_unchecked_mut(i) }),
            None => {
                let mut builder = ChunkBuilder::new();
                tags.configure_chunk(&mut builder);
                components.configure_chunk(&mut builder);

                let chunk_id = self.id.chunk(self.next_chunk_id);
                let chunk_index = self.chunks.len() as ChunkIndex;
                self.next_chunk_id += 1;
                self.chunks.push(builder.build(chunk_id));
                self.version += 1;

                let chunk = self.chunks.last_mut().unwrap();

                debug!(self.logger, "allocated chunk"; "chunk_id" => chunk_id.2);

                (chunk_index, chunk)
            }
        }
    }
}
