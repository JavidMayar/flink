/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.test.example;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.graph.example.EuclideanGraphExample;
import org.apache.flink.graph.example.utils.EuclideanGraphData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

@RunWith(Parameterized.class)
public class EuclideanGraphExampleITCase extends MultipleProgramsTestBase {

	private String verticesPath;

	private String edgesPath;

	private String resultPath;

	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public EuclideanGraphExampleITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();
		File verticesFile = tempFolder.newFile();
		Files.write(EuclideanGraphData.VERTICES, verticesFile, Charsets.UTF_8);

		File edgesFile = tempFolder.newFile();
		Files.write(EuclideanGraphData.EDGES, edgesFile, Charsets.UTF_8);

		verticesPath = verticesFile.toURI().toString();
		edgesPath = edgesFile.toURI().toString();
	}

	@Test
	public void testGraphWeightingExanple() throws Exception {
		EuclideanGraphExample.main(new String[]{verticesPath, edgesPath, resultPath});
		expected = EuclideanGraphData.RESULTED_WEIGHTED_EDGES;
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}
}
