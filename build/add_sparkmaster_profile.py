# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Adds or updates the sparkmaster shim profile in pom.xml.

Usage:
    python3 build/add_sparkmaster_profile.py [--private] <pom_file> <spark_master_version>

Example:
    python3 build/add_sparkmaster_profile.py pom.xml 4.2.0-SNAPSHOT
    python3 build/add_sparkmaster_profile.py --private /path/to/private/pom.xml 4.2.0-SNAPSHOT

The --private flag selects the spark-rapids-private profile template which has
only <module>core</module> and no extra properties (parquet, delta, iceberg, slf4j).

The script is idempotent: if the profile/property already exist, it updates
the version text only. Uses raw-text insertion (not xml.etree) because the
releasemaster profile lives inside a <!-- #if scala-2.13 --> XML comment guard
that xml.etree would strip.
"""

import re
import sys


PROFILE_TEMPLATE_PUBLIC = """\
        <profile>
            <id>releasemaster</id>
            <activation>
                <property>
                    <name>buildver</name>
                    <value>master</value>
                </property>
            </activation>
            <properties>
                <buildver>master</buildver>
                <spark.version>${sparkmaster.version}</spark.version>
                <spark.test.version>${sparkmaster.version}</spark.test.version>
                <parquet.hadoop.version>1.13.1</parquet.hadoop.version>
                <rapids.delta.artifactId1>rapids-4-spark-delta-stub</rapids.delta.artifactId1>
                <slf4j.version>2.0.7</slf4j.version>
            </properties>
            <modules>
                <module>delta-lake/delta-stub</module>
                <module>iceberg/iceberg-stub</module>
            </modules>
        </profile>
"""

PROFILE_TEMPLATE_PRIVATE = """\
        <profile>
            <id>releasemaster</id>
            <activation>
                <property>
                    <name>buildver</name>
                    <value>master</value>
                </property>
            </activation>
            <properties>
                <buildver>master</buildver>
                <spark.version>${sparkmaster.version}</spark.version>
                <spark.test.version>${sparkmaster.version}</spark.test.version>
            </properties>
            <build>
                <pluginManagement>
                    <plugins>
                        <plugin>
                            <groupId>net.alchim31.maven</groupId>
                            <artifactId>scala-maven-plugin</artifactId>
                            <configuration>
                                <release combine.self="override"/>
                                <target>${java.major.version}</target>
                            </configuration>
                        </plugin>
                    </plugins>
                </pluginManagement>
            </build>
            <modules>
                <module>core</module>
            </modules>
        </profile>
"""


def _add_or_update_version_property(lines, version):
    """Insert or update <sparkmaster.version> in the properties block."""
    prop_pattern = re.compile(r'(\s*)<sparkmaster\.version>.*</sparkmaster\.version>')
    anchor_pattern = re.compile(r'(\s*)<spark411\.version>.*</spark411\.version>')

    for i, line in enumerate(lines):
        m = prop_pattern.search(line)
        if m:
            indent = m.group(1)
            lines[i] = "{}<sparkmaster.version>{}</sparkmaster.version>\n".format(indent, version)
            return "updated"

    for i, line in enumerate(lines):
        if anchor_pattern.search(line):
            indent = "        "
            new_line = "{}<sparkmaster.version>{}</sparkmaster.version>\n".format(indent, version)
            lines.insert(i + 1, new_line)
            return "inserted"

    raise RuntimeError("Could not find <spark411.version> anchor in pom.xml")


def _add_or_update_profile(lines, private=False):
    """Insert or replace releasemaster profile inside the scala-2.13 guard, after release411."""
    template = PROFILE_TEMPLATE_PRIVATE if private else PROFILE_TEMPLATE_PUBLIC

    # Check if profile already exists — if so, replace it
    start_idx = None
    end_idx = None
    for i, line in enumerate(lines):
        if '<id>releasemaster</id>' in line:
            # Walk backwards to find <profile> opening
            for j in range(i - 1, -1, -1):
                if '<profile>' in lines[j]:
                    start_idx = j
                    break
            # Walk forwards to find </profile> closing
            for j in range(i + 1, len(lines)):
                if '</profile>' in lines[j]:
                    end_idx = j
                    break
            break

    if start_idx is not None and end_idx is not None:
        lines[start_idx:end_idx + 1] = [template]
        return "updated"

    # Not found — insert after release411's closing </profile>
    release411_found = False
    for i, line in enumerate(lines):
        if '<id>release411</id>' in line:
            release411_found = True
        if release411_found and '</profile>' in line:
            lines.insert(i + 1, template)
            return "inserted"

    raise RuntimeError(
        "Could not find release411 closing </profile> tag in pom.xml"
    )


def add_sparkmaster_profile(pom_file, version, private=False):
    with open(pom_file, 'r') as f:
        lines = f.readlines()

    prop_action = _add_or_update_version_property(lines, version)
    profile_action = _add_or_update_profile(lines, private=private)

    with open(pom_file, 'w') as f:
        f.writelines(lines)

    repo_label = "spark-rapids-private" if private else "spark-rapids"
    actions = []
    if prop_action == "inserted":
        actions.append("added sparkmaster.version property")
    else:
        actions.append("updated sparkmaster.version to {}".format(version))
    if profile_action == "inserted":
        actions.append("added releasemaster profile ({})".format(repo_label))
    elif profile_action == "updated":
        actions.append("updated releasemaster profile ({})".format(repo_label))
    else:
        actions.append("releasemaster profile already present")
    print("; ".join(actions))


if __name__ == "__main__":
    args = sys.argv[1:]
    private = False
    if args and args[0] == "--private":
        private = True
        args = args[1:]
    if len(args) != 2:
        print("Usage: {} [--private] <pom_file> <spark_master_version>".format(sys.argv[0]))
        print("Example: {} pom.xml 4.2.0-SNAPSHOT".format(sys.argv[0]))
        print("         {} --private /path/to/private/pom.xml 4.2.0-SNAPSHOT".format(sys.argv[0]))
        sys.exit(1)
    add_sparkmaster_profile(args[0], args[1], private=private)
