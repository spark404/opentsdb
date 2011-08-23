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

JERSEYNETTY_VERSION := 1.0.0-SNAPSHOT
JERSEYNETTY := third_party/jersey-netty/jersey-netty-$(JERSEYNETTY_VERSION).jar
JERSEYNETTY_BASE_URL := http://www.strocamp.net/opentsdb/thirdparty

$(JERSEYNETTY): $(JERSEYNETTY).md5
	set dummy "$(JERSEYNETTY_BASE_URL)" "$(JERSEYNETTY)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JERSEYNETTY)
