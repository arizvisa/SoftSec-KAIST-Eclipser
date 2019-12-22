namespace Eclipser
open FSharp.Control.Reactive
open Minio

(*
#r "bin/linux-gnu/Debug/netcoreapp3.1/Minio.dll"
#r "bin/linux-gnu/Debug/netcoreapp3.1/System.Reactive.dll"
#r "bin/linux-gnu/Debug/netcoreapp3.1/FSharp.Control.Reactive.dll"
*)

type BucketStore = {
    client : Minio.MinioClient
    bucket : string
}

module S3Bucket = begin
  exception BucketNotExist   

  let BucketBufferSize = 1500

  let connect endpoint name accessKey secretKey =
    let accessKey' = match accessKey with | Some key -> key | None -> System.Environment.GetEnvironmentVariable "MINIO_ACCESS_KEY" in
    let secretKey' = match secretKey with | Some key -> key | None -> System.Environment.GetEnvironmentVariable "MINIO_SECRET_KEY" in
    let client = new Minio.MinioClient (endpoint, accessKey', secretKey') in
    match client.BucketExistsAsync name |> Async.AwaitTask |> Async.RunSynchronously with
    | false -> raise BucketNotExist
    | true -> {
      client = client;
      bucket = name;
    }

  let list store prefix =
    let observed = store.client.ListObjectsAsync (store.bucket, prefix, false) in
    let result = ref List.empty in
    use subscription = Observable.subscribe (fun item -> result := item :: !result) observed in
    Seq.ofList !result

  let info store name =
    client.StatObjectAsync (store.bucket, name) |> Async.AwaitTask |> Async.RunSynchronously
    
  let get store name =
    let result = ref Array.empty in
    let callback (stream : System.IO.Stream) =
      let data = Array.zeroCreate<byte> BucketBufferSize in
      let available = stream.Read (data, 0, Array.length data) in
      result := Array.sub data 0 available |> Array.append !result
    in
    store.client.GetObjectAsync (store.bucket, name, callback) |> Async.AwaitTask |> Async.RunSynchronously
    List.ofArray !result

  let put store name (data : byte array) =
    let stream = new System.IO.MemoryStream (data) in
    store.client.PutObjectAsync (store.bucket, name, stream, int64(Array.length data)) |> Async.AwaitTask |> Async.RunSynchronously
end
