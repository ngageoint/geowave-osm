# Geowave-OSM v 0.1
## About  

<a href="https://travis-ci.org/ngageoint/geowave-osm">
	<img alt="Travis-CI test status" 
	     src="https://travis-ci.org/ngageoint/geowave-osm.svg?branch=master"/>
</a>
<br/>

<a href='https://coveralls.io/r/ngageoint/?branch=master'>
  <img src='https://coveralls.io/repos/ngageoint/geowave-osm/badge.png?branch=master'
       alt='Coverage Status' />
</a>

#Note 
###This code is not fully operational

GeoWave-OSM is intended to be a full data + services stack for the OSM ecosystem.

More information in the design proposal @
https://github.com/ngageoint/geowave/wiki/Support-Full-OSM-Stack-Toolchain

###Status
- [ ] Bulk Ingest
  - [x] PBF   
  - [ ] OSM Xml
- [ ] Diff Support
  - [x] Change set files (formats same as bulk ingest) 
  - [ ] Dirty notification / rendering updates
- [ ] Node/Way/Relation Persistence
  - [x] Persistence of native node/way/relation model (mapped to accumulo)
  - [x] Updateable (from diffs)
  - [ ] Generation of diffs per timespan
  - [x] Full Accumulo authorization support
- [ ] Editing API (OSM v0.6)
  - [ ] Integration with change/dirty notification for diff ingest
  - [ ] Authorization integration
- [ ] Simple Feature Generation	
  -  [x] Map reduce jobs to create simple features from native node/way/relations
  -  [x] Mapping configuration uses user submittable Imposm3 mapping file.
  -  [ ] Integration into change/dirty notification for diffs and edits

## Pull Requests

All pull request contributions to this project will be released under the Apache 2.0 license.  

Software source code previously released under an open source license and then modified by NGA staff is considered a "joint work" (see *17 USC  101*); it is partially copyrighted, partially public domain, and as a whole is protected by the copyrights of the non-government authors and must be released according to the terms of the original open source license.
