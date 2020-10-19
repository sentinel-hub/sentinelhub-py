//VERSION=3

// Calculate number of bands needed for all intervals
// Initialize dates and interval
// Beware: in JS months are 0 indexed
var start_date = new Date(2020, 6, 1, 0, 0, 0);
var end_date = new Date(2020, 6, 30, 0, 0, 0);
var sampled_dates = sample_timestamps(start_date, end_date, 7, 'day').map(d => withoutTime(d));
var nb_bands = sampled_dates.length;
var n_valid = 0;
var n_all = 0;

function interval_search(x, arr) {
  let start_idx = 0,  end_idx = arr.length - 2;

  // Iterate while start not meets end
  while (start_idx <= end_idx) {
    // Find the mid index
    let mid_idx = (start_idx + end_idx) >> 1;

    // If element is present at mid, return True
    if (arr[mid_idx] <= x && x < arr[mid_idx + 1]) {
      return mid_idx;
    }
    // Else look in left or right half accordingly
    else if (arr[mid_idx + 1] <= x) start_idx = mid_idx + 1;
    else end_idx = mid_idx - 1;
  }
  if (x == arr[arr.length-1]){
    return arr.length-2;
  }
  return undefined;
}

function linearInterpolation(x, x0, y0, x1, y1, no_data_value=NaN) {
  if (x < x0 || x > x1) {
    return no_data_value;
  }
  var a = (y1 - y0) / (x1 - x0);
  var b = -a * x0 + y0;
  return a * x + b;
}

function lininterp(x_arr, xp_arr, fp_arr, no_data_value=NaN) {
  results = [];
  data_mask = [];
  xp_arr_idx = 0;
  for (var i=0; i<x_arr.length; i++) {
    var x = x_arr[i];
    n_all+=1;
    interval = interval_search(x, xp_arr);
    if (interval === undefined) {
      data_mask.push(0);
      results.push(no_data_value);
      continue;
    }
    data_mask.push(1);
    n_valid+=1;
    results.push(
      linearInterpolation(
        x,
        xp_arr[interval],
        fp_arr[interval],
        xp_arr[interval+1],
        fp_arr[interval+1],
        no_data_value
      )
    );
  }
  return [results, data_mask];
}

function interpolated_index(index_a, index_b) {
  // Calculates the index for all bands in array
  var index_data = [];
  for (var i = 0; i < index_a.length; i++){
     // UINT index returned
     let ind = (index_a[i] - index_b[i]) / (index_a[i] + index_b[i]);
     index_data.push(ind * 10000 + 10000);
  }
  return index_data
}

function increase(original_date, period, period_unit) {
    date = new Date(original_date)
    switch (period_unit) {
        case 'millisecond':
            return new Date(date.setMilliseconds(date.getMilliseconds()+period));
        case 'second':
            return new Date(date.setSeconds(date.getSeconds()+period));
        case 'minute':
            return new Date(date.setMinutes(date.getMinutes()+period));
        case 'hour':
            return new Date(date.setHours(date.getHours()+period));
        case 'day':
            return new Date(date.setDate(date.getDate()+period));
        case 'month':
            return new Date(date.setMonth(date.getMonth()+period));
        default:
            return undefined
    }
}

function sample_timestamps(start, end, period, period_unit) {
    var cDate = new Date(start);
    var sampled_dates = []
    while (cDate < end) {
        sampled_dates.push(cDate);
        cDate = increase(cDate, period, period_unit);
    }
    return sampled_dates;
}

function is_valid(smp) {
  // Check if the sample is valid (i.e. contains no clouds or snow)
  let clm = smp.CLM;
  let dm = smp.dataMask;

  if (clm === 1 || clm === 255) {
        return false;
  }
  if (dm !=1 ) {
        return false;
  }
  return true;
}

function withoutTime(intime) {
  // Return date without time
  intime.setHours(0, 0, 0, 0);
  return intime;
}

// Sentinel Hub functions
function setup() {
  // Setup input/output parameters
    return {
        input: [{
            bands: ["B04", "B08", "CLM", "dataMask"],
            units: "DN"
        }],
      output: [
          {id: "NDVI", bands: nb_bands, sampleType: SampleType.UINT16},
          {id: "data_mask", bands: nb_bands, sampleType: SampleType.UINT8}
      ],
    mosaicking: "ORBIT"
    }
}

// Evaluate pixels in the bands
function evaluatePixel(samples, scenes) {

  // Initialise arrays
  var valid_samples = {'B04':[], 'B08':[]};

  var valid_dates = []
  // Loop over samples.
  for (var i = samples.length-1; i >= 0; i--){
      if (is_valid(samples[i])) {
        valid_dates.push(withoutTime(new Date(scenes[i].date)));
        valid_samples['B04'].push(samples[i].B04);
        valid_samples['B08'].push(samples[i].B08);
      }
  }

  // Calculate indices and return optimised for UINT16 format (will need unpacking)
  var ndvi = interpolated_index(valid_samples['B08'], valid_samples['B04'])

  var [ndvi_interpolated, dm] = lininterp(sampled_dates, valid_dates, ndvi, 0);

  // Return all arrays
  return {
    NDVI: ndvi,
    data_mask: dm
  }
}
