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

package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.sql.Types;

/**
 * Skeleton code for implementing a fraud detector.
 * The KeyedProcessFunction provides control over both state and time. (if want to access keyed state and timers
 * then it has to be applied to a keyed stream, e.g. stream.keyBy(..).KeyProcessFunction()).
 * Access to state is via RuntimeContext.
 *
 * Every call to processElement(...) gets a Context object which gives access to
 * - event timestamp
 * - TimerService (used to register callbacks for future event- or process-time instants.
 *
 * For event-time timers.
 * onTime() is called when the current watermark is advanced up to or beyond the timestamp of the timer.
 *
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;
	private transient ValueState<Boolean> flagState;
	private transient ValueState<Long> timerState;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Boolean.class);
		flagState = getRuntimeContext().getState(flagDescriptor);

		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer-state", Long.class);
		timerState = getRuntimeContext().getState(timerDescriptor);
	}


	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
		// remove flag after 1 minute
		timerState.clear();
		flagState.clear();
	}



	private void cleanUp(Context ctx) throws Exception {
		// delete timer
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);

		// clean up all state
		timerState.clear();
		flagState.clear();
	}



	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		// current state for current key
		Boolean lastTransactionWasSmall = flagState.value();

		if (lastTransactionWasSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());
				collector.collect(alert);
			}

			cleanUp(context);  // reset state
		}

		if (transaction.getAmount() < SMALL_AMOUNT) {
			flagState.update(true);

			// set the timer and timer state
			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			context.timerService().registerProcessingTimeTimer(timer);
			timerState.update(timer);
		}


	}
}
