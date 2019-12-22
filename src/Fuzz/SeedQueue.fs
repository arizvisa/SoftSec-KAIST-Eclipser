namespace Eclipser

open MBrace.FsPickler
open Config
open Utils
open Options

type ConcolicQueue = {
  FavoredQueue : Queue<Seed>
  NormalQueue : FileQueue
}

module ConcolicQueue =

  let serializer = FsPickler.CreateBinarySerializer ()

  let initialize queueDir favoredQueuePath =
    { FavoredQueue = Queue.load favoredQueuePath
      NormalQueue = FileQueue.initialize "concolic-seed" queueDir }

  let save queue path = Queue.save queue.FavoredQueue path

  let isEmpty queue =
    Queue.isEmpty queue.FavoredQueue && FileQueue.isEmpty queue.NormalQueue

  let enqueue queue (priority, seed) =
    match priority with
    | Favored ->
      let newFavoredQueue = Queue.enqueue queue.FavoredQueue seed
      { queue with FavoredQueue = newFavoredQueue }
    | Normal ->
      let seedBytes = serializer.Pickle seed
      let newNormalQueue = FileQueue.enqueue queue.NormalQueue seedBytes
      { queue with NormalQueue = newNormalQueue }

  let dequeue queue =
    let favoredQueue = queue.FavoredQueue
    let normalQueue = queue.NormalQueue
    let queueSelection =
      if FileQueue.isEmpty normalQueue then Favored
      elif Queue.isEmpty favoredQueue then Normal
      else Favored
    match queueSelection with
    | Favored ->
      let seed, newFavoredQueue = Queue.dequeue favoredQueue
      let newQueue = { queue with FavoredQueue = newFavoredQueue }
      (Favored, seed, newQueue)
    | Normal ->
      let seedBytes, newNormalQueue = FileQueue.dequeue normalQueue
      let seed = serializer.UnPickle<Seed> seedBytes
      let newQueue = { queue with NormalQueue = newNormalQueue }
      (Normal, seed, newQueue)

type RandFuzzQueue = {
  FavoredQueue : DurableQueue<Seed>
  NormalQueue : FileQueue
  LastMinimizedCount : int
  RemoveCount : int
}

module RandFuzzQueue =

  let serializer = FsPickler.CreateBinarySerializer ()

  let initialize queueDir dummyQueuePath =
    let dummyQueue =
      if System.IO.File.Exists dummyQueuePath then
        DurableQueue.load dummyQueuePath
      else
        let dummySeed = Seed.make Args [] 0 0 in
        DurableQueue.initialize dummySeed
    in
    let fileQueue = FileQueue.initialize "rand-seed" queueDir
    { FavoredQueue = dummyQueue
      NormalQueue = fileQueue
      LastMinimizedCount = 0
      RemoveCount = 0 }

  let save queue path = DurableQueue.save queue.FavoredQueue path

  let enqueue queue (priority, seed) =
    match priority with
    | Favored ->
      let newFavoredQueue = DurableQueue.enqueue queue.FavoredQueue seed
      { queue with FavoredQueue = newFavoredQueue }
    | Normal ->
      let seedBytes = serializer.Pickle seed
      let newNormalQueue = FileQueue.enqueue queue.NormalQueue seedBytes
      { queue with NormalQueue = newNormalQueue }

  let dequeue queue =
    let favoredQueue = queue.FavoredQueue
    let normalQueue = queue.NormalQueue
    let queueSelection =
      if FileQueue.isEmpty normalQueue then Favored
      elif random.NextDouble() < FavoredSeedProb then Favored
      else Normal
    match queueSelection with
    | Favored ->
      let seed, newFavoredQueue = DurableQueue.fetch favoredQueue
      let newQueue = { queue with FavoredQueue = newFavoredQueue }
      (Favored, seed, newQueue)
    | Normal ->
      let seedBytes, newNormalQueue = FileQueue.dequeue normalQueue
      let seed = serializer.UnPickle<Seed> seedBytes
      let newQueue = { queue with NormalQueue = newNormalQueue }
      (Normal, seed, newQueue)

  let timeToMinimize queue =
    let curSize = queue.FavoredQueue.Count
    let prevSize = queue.LastMinimizedCount
    curSize > int (float prevSize * SeedCullingThreshold)

  let rec findRedundantsGreedyAux queue seedEntries accRedundantSeeds =
    if List.isEmpty seedEntries then accRedundantSeeds
    else
      (* Choose an entry that has largest number of covered nodes *)
      let getNodeCount (idx, seed, nodes) = Set.count nodes
      let seedEntriesSorted = List.sortByDescending getNodeCount seedEntries
      let _, _, chosenNodes = List.head seedEntriesSorted
      let seedEntries = List.tail seedEntriesSorted
      (* Now update (i.e. subtract node set) seed entries *)
      let subtractNodes nodes (i, s, ns) = (i, s, Set.difference ns nodes)
      let seedEntries = List.map (subtractNodes chosenNodes) seedEntries
      (* If the node set entry is empty, it means that seed is redundant *)
      let redundantEntries, seedEntries =
        List.partition (fun (i, s, ns) -> Set.isEmpty ns) seedEntries
      let redundantSeeds = List.map (fun (i, s, _) -> (i, s)) redundantEntries
      let accRedundantSeeds = redundantSeeds @ accRedundantSeeds
      findRedundantsGreedyAux queue seedEntries accRedundantSeeds

  let findRedundantsGreedy seeds queue opt =
    let seedEntries =
      List.fold
        (fun accSets (idx, seed) ->
          (idx, seed, Executor.getNodeSet opt seed) :: accSets
        ) [] seeds
    findRedundantsGreedyAux queue seedEntries []

  let minimize queue opt =
    let favoredQueue = queue.FavoredQueue
    let seeds = favoredQueue.Elems.[0 .. favoredQueue.Count - 1]
                |> Array.mapi (fun i seed -> (i, seed))
                |> List.ofArray
    let seedsToRemove = findRedundantsGreedy seeds favoredQueue opt
    // Note that we should remove larger index first
    let newFavoredQueue = List.sortByDescending fst seedsToRemove
                          |> List.fold DurableQueue.remove favoredQueue
    { queue with FavoredQueue = newFavoredQueue
                 LastMinimizedCount = newFavoredQueue.Count
                 RemoveCount = queue.RemoveCount + seedsToRemove.Length }

