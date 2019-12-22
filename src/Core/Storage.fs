namespace Eclipser

open FSharp.Control.Reactive
open Minio

type BucketStore = {
    client : Minio.MinioClient
    bucket : string
}

module S3Bucket = begin
  exception BucketNotExist   

  let connect endpoint name accessKey secretKey =
    let accessKey' = match accessKey with | Some key -> key | None -> System.Environment.GetEnvironmentVariable "MINIO_ACCESS_KEY" in
    let secretKey' = match secretKey with | Some key -> key | None -> System.Environment.GetEnvironmentVariable "MINIO_SECRET_KEY" in
    let client = new Minio.MinioClient (endpoint, accessKey', secretKey') in
    match client.BucketExistsAsync name |> Async.AwaitTask |> Async.RunSynchronously with
    | false -> raise BucketNotExist
    | true -> { client = client; bucket = name }

  let list store prefix =
    let observed = store.client.ListObjectsAsync (store.bucket, prefix, false) in
    Observable.toEnumerable observed |> List.ofSeq

  let info store name =
    store.client.StatObjectAsync (store.bucket, name) |> Async.AwaitTask |> Async.RunSynchronously
    
  let get store name =
    let result = new System.IO.MemoryStream () in
    let callback (stream : System.IO.Stream) =
      stream.CopyTo (result)
    in
    store.client.GetObjectAsync (store.bucket, name, callback) |> Async.AwaitTask |> Async.RunSynchronously
    result.GetBuffer()

  let put store name (data : byte array) =
    let stream = new System.IO.MemoryStream (data) in
    store.client.PutObjectAsync (store.bucket, name, stream, int64(Array.length data)) |> Async.AwaitTask |> Async.RunSynchronously

  let del store (name : string) =
    store.client.RemoveObjectAsync (store.bucket, name) |> Async.AwaitTask |> Async.RunSynchronously
end
