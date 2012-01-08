package com.google.caliper.runner;

/*import com.google.caliper.model.Measurement;
import com.google.caliper.model.Result;
import com.google.caliper.model.VM;
import com.google.common.base.Function;
import com.google.common.primitives.Doubles;

import java.util.Arrays;
import java.util.Collection;*/

/**
 * User: remeniuk
 */
public class SimplyProcessedResult {
  /*private final Result modelResult;
  private final double[] values;
  private final double min;
  private final double max;
  private final double median;
  private final double mean;
  private final String responseUnit;
  private final String responseDesc;

  public SimplyProcessedResult(Result modelResult) {
    this.modelResult = modelResult;
    values = getValues(modelResult.measurements);
    min = Doubles.min(values);
    max = Doubles.max(values);
    median = computeMedian(values);
    mean = computeMean(values);
    Measurement firstMeasurement = modelResult.measurements.get(0);
    responseUnit = firstMeasurement.unit;
    responseDesc = firstMeasurement.description;
  }

    public Result getModelResult() {
        return modelResult;
    }

    public double[] getValues() {
        return values;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getMedian() {
        return median;
    }

    public double getMean() {
        return mean;
    }

    public String getResponseUnit() {
        return responseUnit;
    }

    public String getResponseDesc() {
        return responseDesc;
    }

    private double[] getValues(Collection<Measurement> measurements) {
    double[] values = new double[measurements.size()];
    int i = 0;
    for (Measurement measurement : measurements) {
      values[i] = measurement.value / measurement.weight;
      i++;
    }
    return values;
  }

  // TODO(schmoe): consider copying com.google.math.Sample into caliper.util
  private double computeMedian(double[] values) {
    double[] sortedValues = values.clone();
    Arrays.sort(sortedValues);
    if (sortedValues.length % 2 == 1) {
      return sortedValues[sortedValues.length / 2];
    } else {
      double high = sortedValues[sortedValues.length / 2];
      double low = sortedValues[(sortedValues.length / 2) - 1];
      return (low + high) / 2;
    }
  }

  private double computeMean(double[] values) {
    double sum = 0;
    for (double value : values) {
      sum += value;
    }
    return sum / values.length;
  } */
}

