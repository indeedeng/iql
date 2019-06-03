iql
=======
Web interface for making IQL queries against an Imhotep cluster.

## Branches
master - stable version that works on AWS. If you are outside Indeed, this is the recommended branch to use.

internal - internal version that has many more changes but does not compile publicly due to a missing dependency on 
ImhotepMetadataService library which is not yet open source.

noims - branched from internal and can build in open source due to the ImhotepMetadataService dependency being removed. 
Not tested on AWS.

## Documentation
See the [overview](http://indeedeng.github.io/imhotep/docs/overview/) to learn about IQL syntax. 

## Discussion
Ask and answer questions in our Q&A forum for Imhotep: [indeedeng-imhotep-users](https://groups.google.com/forum/#!forum/indeedeng-imhotep-users)

## Related Projects
[imhotep](https://github.com/indeedeng/imhotep): Core Imhotep code

[iupload](https://github.com/indeedeng/iupload): TSV uploader webapp for an Imhotep cluster

[imhotep-tsv-converter](https://github.com/indeedeng/imhotep-tsv-converter): Tool to convert TSV files into Flamdex indexes for Imhotep

[imhotep-cloudformation](https://github.com/indeedeng/imhotep-cloudformation): Cloudformation scripts and other helper scripts for spinning up an Imhotep cluster in AWS

## Contributing
http://indeedeng.github.io/imhotep/docs/contributing/

## License

[Apache License Version 2.0](https://github.com/indeedeng/imhotep/blob/master/LICENSE)
