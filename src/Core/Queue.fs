namespace Eclipser

open Config
open MBrace.FsPickler

/// A simple, purely functional queue.
type Queue<'a > = {
  Enqueued : 'a list
  ToDequeue : 'a list
}

module Queue =

  exception EmptyException

  let serializer = FsPickler.CreateBinarySerializer ()

  let empty = { Enqueued = []; ToDequeue = [] }

  let load<'a> path =
    match System.IO.File.Exists path with
    | false -> empty
    | true ->
      let data = System.IO.File.ReadAllBytes(path) in
      serializer.UnPickle<Queue<'a>> data

  let save q path =
    let data = serializer.Pickle q in
    System.IO.File.WriteAllBytes(path, data)

  let getSize q = q.Enqueued.Length + q.ToDequeue.Length

  let isEmpty q = List.isEmpty q.Enqueued && List.isEmpty q.ToDequeue

  /// Enqueue an element to a queue.
  let enqueue q elem = { q with Enqueued = elem :: q.Enqueued }

  /// Dequeue an element from a queue. Raises Queue.EmptyException if the
  /// queue is empty.
  let dequeue q =
    match q.Enqueued, q.ToDequeue with
    | [], [] -> raise EmptyException
    | enq, [] ->
      let enqRev = List.rev enq
      let elem, toDequeue = List.head enqRev, List.tail enqRev
      (elem, { Enqueued = []; ToDequeue = toDequeue })
    | enq, deqHd :: deqTail ->
      (deqHd, { Enqueued = enq; ToDequeue = deqTail })

  /// Peek an element from a queue, without actually removing it from the queue.
  let peek q =
    match q.Enqueued, q.ToDequeue with
    | [], [] -> raise EmptyException
    | enq, [] -> List.head (List.rev enq)
    | _, deqHd :: _ -> deqHd

  /// Drop the next element from a queue and return a shrunken queue.
  let drop q =
    match q.Enqueued, q.ToDequeue with
    | [], [] -> raise EmptyException
    | enq, [] -> { Enqueued = []; ToDequeue = List.tail (List.rev enq) }
    | enq, _ :: deqTail -> { Enqueued = enq; ToDequeue = deqTail }

  let elements q =
    q.ToDequeue @ (List.rev q.Enqueued)

  let toString stringfy q =
    let elems = q.ToDequeue @ (List.rev q.Enqueued)
    let elemStrs = List.map stringfy elems
    String.concat "\n" elemStrs

/// A durable queue, where elements are not volatile. Elements are fetched out
/// in a round-robin manner, but the elements are not removed from the queue
/// unless explicitly requested.
type DurableQueue<'a> = {
  Elems : 'a array
  Count : int
  Finger : int // Next element to fetch.
}

module DurableQueue =

  exception EmptyException
  exception InvalidFingerException

  let serializer = FsPickler.CreateBinarySerializer ()

  let initialize initElem =
    { Elems = Array.create DurableQueueMaxSize initElem
      Count = 0
      Finger = 0 }

  let save queue path =
    let data = serializer.Pickle queue in
    System.IO.File.WriteAllBytes(path, data)

  let load<'a> path =
    let data = System.IO.File.ReadAllBytes(path) in
    serializer.UnPickle<DurableQueue<'a>> data

  let getSize queue = queue.Count

  let isEmpty queue =
    queue.Count = 0

  let private isFull queue =
    queue.Count >= queue.Elems.Length

  /// Enqueue an element to a queue. If the queue is already full, return the
  /// same queue.
  let enqueue queue elem =
    if isFull queue then queue
    else queue.Elems.[queue.Count] <- elem
         { queue with Count = queue.Count + 1 }

  /// Remove an element from the queue. This functionality is useful when queue
  /// should be managed in a compactly minimized state (e.g. seed culling).
  let remove queue (idx, elem) =
    if queue.Elems.[idx] <> elem then failwith "Element array messed up"
    let shiftElem i = queue.Elems.[i] <- queue.Elems.[i + 1]
    List.iter shiftElem (List.ofSeq { idx .. (queue.Count - 2) })
    let finger = queue.Finger
    let newFinger = if idx < finger then finger - 1 else finger
    let newCount = queue.Count - 1
    { queue with Finger = newFinger; Count = newCount }

  /// Fetch an element from the queue, while not removing the fetched element
  /// from the queue. Raises DurableQueue.EmptyException if the queue is empty.
  let fetch queue =
    if queue.Count = 0 then raise EmptyException
    if queue.Finger >= queue.Count then raise InvalidFingerException
    let elem = queue.Elems.[queue.Finger]
    let newFinger = (queue.Finger + 1) % queue.Count
    let newQueue = { queue with Finger = newFinger }
    (elem, newQueue)

/// A file-system-based queue. Queue element is always a byte array (use Pickle
/// to encode a value into a byte array).
type FileQueue = {
  Name : string
  Directory : string
  LowerIdx : int
  UpperIdx : int
  Finger : int
  MaxCount : int
}

module FileQueue =

  exception EmptyException
  exception InvalidFingerException

  let create name directory =
    ignore (System.IO.Directory.CreateDirectory(directory))
    { Name = name
      Directory = directory
      UpperIdx = 0
      LowerIdx = 0
      Finger = 0
      MaxCount = FileQueueMaxSize }

  let load name directory =
    if not (System.IO.Directory.Exists directory) then
      raise (System.IO.DirectoryNotFoundException directory)
    let splitpath (ch : char) (path : string) =
      let dir = System.IO.Path.GetDirectoryName(path) in
      let name = System.IO.Path.GetFileName(path) in
      match name.Split(ch, 2) with
      | [| prefix; suffix |] -> (dir, prefix, suffix)
      | _ -> raise (failwith "Unexpected pattern")
    in
    let getnumber (s : string) =
      let result = ref 0 in
      if System.Int32.TryParse(s, result) then
        Some (unbox<int> !result)
      else
        None
    in
    let indices =
      System.IO.Directory.EnumerateFiles directory |>
      Seq.map (splitpath '-') |>
      Seq.choose (
        fun (dir, prefix, suffix) ->
          if dir = directory && prefix = name then
            getnumber suffix
          else
            None
      ) |> Seq.sort
    in {
      Name = name
      Directory = directory
      UpperIdx = if Seq.isEmpty indices then 0 else Seq.max indices
      LowerIdx = if Seq.isEmpty indices then 0 else Seq.min indices
      Finger = if Seq.isEmpty indices then 0 else Seq.min indices
      MaxCount = FileQueueMaxSize
    }

  let initialize name directory =
    if System.IO.Directory.Exists(directory) then
        load name directory
    else
        create name directory

  let getSize queue =
    queue.UpperIdx - queue.LowerIdx

  let private isFull queue =
    queue.MaxCount <= getSize queue

  let isEmpty queue =
    getSize queue = 0

  let private isValidFinger queue =
    queue.LowerIdx <= queue.Finger && queue.Finger < queue.UpperIdx

  let add queue elemBytes =
    let filePath = sprintf "%s/%s-%d" queue.Directory queue.Name queue.UpperIdx
    System.IO.File.WriteAllBytes(filePath, elemBytes)
    let newUpperIdx = queue.UpperIdx + 1
    { queue with UpperIdx = newUpperIdx }

  /// Enqueue an element to a queue. If the queue is already full, return the
  /// same queue.
  let enqueue queue elemBytes =
    if isFull queue then queue else add queue elemBytes

  /// Dequeue an element from a queue. Raises FileQueue.EmptyException if the
  /// queue is empty.
  let dequeue queue =
    if isEmpty queue then raise EmptyException
    if not (isValidFinger queue) then raise InvalidFingerException
    let lowerIdx = queue.LowerIdx
    let finger = queue.Finger
    let filePath = sprintf "%s/%s-%d" queue.Directory queue.Name lowerIdx
    let elemBytes = System.IO.File.ReadAllBytes(filePath)
    System.IO.File.Delete filePath
    let newLowerIdx = lowerIdx + 1
    let newFinger = max newLowerIdx finger
    let newQueue = { queue with Finger = newFinger; LowerIdx = newLowerIdx }
    (elemBytes, newQueue)