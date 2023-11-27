### Geohash-based/grid-based HeadIndex Parameters
Assume the precision used to normalize longitude and latitude is 21bit, then we have
- 1 integer unit for longitude is about 0.00017 (360 / 2^21)
- 1 integer unit for latitude is about 0.000085 (180 / 2^21)

#### shiftLength
This parameter determines the number of integer unit in longitude/latitude dimensions.
For example, if shiftLength is 16, the grid size is 2^8 x 2^8

#### postingListCapacity
This parameter indicates the maximum moving objects in a grid

#### Example

- when shift length is 8, each grid size: 0.00272 (2^4 * 0.00017)(lon) x 0.00126 (2^4 * 0.000085)(lat)
  - 1 lon unit x 1 lat unit contains 291783 grids
- when shift length is 16, each grid size: 0.04352 (2^8 * 0.00017)(lon) x 0.02176 (2^8 * 0.000085)(lat)
  - 1 lon unit x 1 lat unit contains 1055 grids
- when shift length is 32, each grid size: 11.14 (lon) x 5.57 (lat)
  - 1 lon unit x 1 lat unit contains 0.016 grids

### more general case
assume original x range is [x1, x2], y range is [y1, y2], and the precision used to normalize is n bits, then we have
- 1 integer unit for x is |x2 - x1| / 2^n
- 1 integer unit for y is |y2 - y1| / 2^n