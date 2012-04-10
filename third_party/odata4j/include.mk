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

ODATA4J_VERSION := 0.6.0
ODATA4J := third_party/odata4j/odata4j-$(ODATA4J_VERSION)-nojpabundle.jar
ODATA4J_BASE_URL := http://www.strocamp.net/opentsdb/thirdparty

$(ODATA4J): $(ODATA4J).md5
	set dummy "$(ODATA4J_BASE_URL)" "$(ODATA4J)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(ODATA4J)
