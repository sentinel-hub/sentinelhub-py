//VERSION=3

function setup() {
  return {
    input: [{
      bands: ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12", "SCL", "dataMask"],
      units: "DN"
    }],
    output: [
      {
        id: "bands",
        bands: ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12"],
        sampleType: "UINT16"
      },
      {
        id: "masks",
        bands: ["SCL"],
        sampleType: "UINT16"
      },
      {
        id: "indices",
        bands: ["NDVI", "NDVI_RE1", "NBSI"],
        sampleType: "UINT16"
      },
      {
        id: "dataMask",
        bands: 1
      }]
  }
}

function evaluatePixel(samples) {
    // Normalised Difference Vegetation Index and variation
    let NDVI = index(samples.B08, samples.B04);
    let NDVI_RE1 = index(samples.B08, samples.B05);

    // Bare Soil Index
    let NBSI = index((samples.B11 + samples.B04), (samples.B08 + samples.B02));

    // masking cloudy pixels
    const clouds = [0, 3, 7, 8, 9, 10];
    let combinedMask = samples.dataMask
    if (clouds.includes(samples.SCL)) {
        combinedMask = 0;
    }

    const f = 5000;
    return {
        bands: [samples.B01, samples.B02, samples.B03, samples.B04, samples.B05, samples.B06,
                samples.B07, samples.B08, samples.B8A, samples.B09, samples.B11, samples.B12],
        masks: [samples.CLM],
        indices: [toUINT(NDVI, f), toUINT(NDVI_RE1, f), toUINT(NBSI, f)],
        dataMask: [combinedMask]
    };
}

function toUINT(product, constant){
  // Clamp the output to [-1, 10] and convert it to a UNIT16
  // value that can be converted back to float later.
  if (product < -1) {
    product = -1;
  } else if (product > 10) {
    product = 10;
  }
  return Math.round(product * constant) + constant;
}
