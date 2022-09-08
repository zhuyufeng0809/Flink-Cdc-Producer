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

package org.awesome.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.awesome.flink.bean.conf.Configurations;
import org.awesome.flink.bean.conf.Branch;

public class Job {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Configurations configuration = Configurations.fromJson(args);
		configuration.applyDefaultFlinkConf(env);
		for (Branch branch : configuration.getBranches()) {
			String branchId = branch.getBranchId();
			int parallelism = branch.getParallelism();

			env.fromSource(branch.buildMysqlCdcSource(),
					WatermarkStrategy.noWatermarks(),
					"Mysql Cdc " + branchId)
			.setParallelism(parallelism)
			.sinkTo(branch.buildKafkaSink(configuration.getDefaultFlinkKafkaSinkConf()))
			.setParallelism(parallelism)
			.name("Kafka " + branchId);
		}

		env.execute("Flink Cdc Producer");
	}
}
