# Copyright (C) 2011  The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

JERSEY_VERSION := 1.8
JERSEY_BASE_URL := http://www.strocamp.net/opentsdb/thirdparty

JERSEYCORE := third_party/jersey/jersey-core-$(JERSEY_VERSION).jar
JERSEYSERVER := third_party/jersey/jersey-server-$(JERSEY_VERSION).jar

$(JERSEYCORE): $(JERSEYCORE).md5
	set dummy "$(JERSEY_BASE_URL)" "$(JERSEYCORE)"; shift; $(FETCH_DEPENDENCY)

$(JERSEYSERVER): $(JERSEYSERVER).md5
	set dummy "$(JERSEY_BASE_URL)" "$(JERSEYSERVER)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JERSEYCORE) $(JERSEYSERVER)
