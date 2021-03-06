# Mastersync, master-master replication

Work in progress.

First phase is to replicate MongoDB collections to other
MongoDB instances.

Key features:
* master-master replication like CouchDB but with transformations
* built on the principles of owning your own data
* privacy by design
* [security by design](https://github.com/Netsend/mastersync/wiki/Mastersync-privilege-separation)
* built for and on MongoDB (I hope to remove the MongoDB dependency in a later stage)

See [wiki](https://github.com/Netsend/mastersync/wiki) for progress.

# License

Copyright 2014, 2015 Netsend.

This file is part of Mastersync.

Mastersync is free software: you can redistribute it and/or modify it under the
terms of the GNU Affero General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

Mastersync is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along
with Mastersync. If not, see <https://www.gnu.org/licenses/>.
