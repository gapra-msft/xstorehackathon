# Hackathon for XStore

Welcome! We all have been busy at work trying to make the best experiences for our customers. But every feature travels through many hands before they reach the customer. And sometimes we never get to see the end product. This Hackathon is a chance to walk a mile in our customers shoes and celebrate the work we do.

``` Note: Everything in this repo is in the public domain. ```

## What do you need to get started

* A computer that can host an editor
* A storage account
* You hacking hat

## Things to watch out for

* Do not checkin your SAS token/SharedKey Credential or any other creds

## Recommended Languages (alphabetically)

* .Net
* Java
* JS
* Python

## Some ideas for things to try (don't hesitate in asking for samples)



| Project	|.Net  	| Java 	| JS 	| Python 	|
|---	|---	|---	|---	|---	|
|  [List Million Blobs](ListMillion.md) 	|   2	|   2	|  3 	|  3 	|
|  [List Million Blobs in a non-alphabetic randomized order (extra credits for bigger randomizations)](ListMillionRandom.md) 	|   3	|   3	|   3	|   3	|
|   [Azure Function that does LCM at large scale](LCMFunction.md)	|   3	|   3	|  3 	|   3	|
|   [Setup AAD and use it to transfer data from one account to another](AADChallenge.md)	|   3	|   3	|   3	|   3	|
|   [Download PageBlob ignoring pages that are empty](PageBlobDataSaver.md)	|   4	|   4	|   4	|   4	|
|   [Copy a VHD from one account to another](PageBlobS2S.md)	|   3	|   3	|   3	|   3	|
|   [Using Batch API delete all the data in your account with error handling](BatchDelete.md)	|   2	|   2	|   2	|   2	|
|   [Compare the performance across DFS endpoint and Blob endpoint for same file](Performance.md)	|   2	|   2	|   2	|   2	|
|   [Create a azcopy wrapper to start transfers and listen to job status](azcopy.md)	|   2	|   3	|   3	|   4	|
|   [Use BlobFuse on WSL2.0 and file bugs (4 points)](fuse.md)	|   	|   	|   	|   	|
|   [Create a tool that will help users manage blob versioning (keep/delete/add versions)](Versioning.md)	|   2	|   3	|   3	|   3	|
|   [Tool/API to copy between blobs and files service (extra credits for preserving metadata/smb info)](S2SChallenge.md)	|   3	|   3	|   3	|   3	|
|   [Write a tool that provides filtering capability using Blob Tags](Tags.md)	|   3	|   3	|   3	|   3	|
|   [Write a tool that calculates capacity usage of storage account](Capacity.md)	|   3	|   3	|   3	|   3	|
|   [Throttling challenge](Throttling.md)	|   6	|   6	|   6	|  6	|

## Rules

* It can be a team or a individual event, *you choose*.
* If you are doing your own challenge, add a README.
* Visual Studio subscription give everyone an Azure subscription to play with. Use those credits for testing (my.visualstudio.com)
* Fork this repo and develop at your comfort
* Submit PR to the main branch of this repo (IM to the meeting chat with your PR)
* Name the project appropriately if picking from the above list of challenges
* PR deadline February 6th 2021
* We will provide PR feedback and assign points. Max points listed in table
* One winner is the person with maximum point.
* Another winner will be DevEx team voted best project.

## SDK location

| Service	|Blobs  	| Files 	| Queues 	| ADLS Gen2 	|
|---	|---	|---	|---	|---	|
| .Net  	|   [Link](https://www.nuget.org/packages/Azure.Storage.Blobs/12.8.0)	|   [Link](https://www.nuget.org/packages/Azure.Storage.Files.Shares/12.6.0)	|   [Link](https://www.nuget.org/packages/Azure.Storage.Queues/12.6.0)	|   [Link](https://www.nuget.org/packages/Azure.Storage.Files.DataLake/12.6.0)	|
|  Java 	|  [Link](https://search.maven.org/artifact/com.azure/azure-storage-blob/12.10.0/jar) 	|   [Link](https://search.maven.org/artifact/com.azure/azure-storage-file-share/12.8.0/jar)	|  [Link](https://search.maven.org/artifact/com.azure/azure-storage-queue/12.8.0/jar) 	|   [Link](https://search.maven.org/artifact/com.azure/azure-storage-file-datalake/12.4.0/jar)	|
|  JS 	|   [Link](https://www.npmjs.com/package/@azure/storage-blob)	|   [Link](https://www.npmjs.com/package/@azure/storage-file-share)	|   [Link](https://www.npmjs.com/package/@azure/storage-queue/v/12.2.0)	|   [Link](https://www.npmjs.com/package/@azure/storage-file-datalake)	|
|  Python 	|   [Link](https://pypi.org/project/azure-storage-blob/12.7.0/)	|   [Link](https://pypi.org/project/azure-storage-file-share/12.4.0/)	|   [Link](https://pypi.org/project/azure-storage-queue/12.1.4/)	|   [Link](https://pypi.org/project/azure-storage-file-datalake/12.2.0/)	|