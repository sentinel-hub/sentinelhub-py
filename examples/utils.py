from ipyleaflet import Map, DrawControl, Marker, Rectangle
from sentinelhub import BBox, CRS

from ipywidgets import widgets as w


class BboxSelector:
    def __init__(self, center=(0, 0), zoom=2):
        control = DrawControl()

        control.rectangle = {
            "shapeOptions": {
                "fillColor": "#fca45d",
                "color": "#fca45d",
                "fillOpacity": 0.5
            }
        }

        #Disable the rest of draw options
        control.polyline = {}
        control.circle = {}
        control.circlemarker = {}
        control.polygon = {}

        control.on_draw(self._handle_draw)

        self.map = Map(center=(53, 354), zoom=5, scroll_wheel_zoom=True)
        self.map.add_control(control)

        self.rectangle = Rectangle(bounds=((52, 354), (53, 360)))
        self.map.add_layer(self.rectangle)

        self.bbox = None
        self.size = None

        # self.out = w.Output(layout=w.Layout(width='100%', height='15px', overflow_y='scroll'))
        # self.vbox = w.VBox([self.map, self.out])

    def _handle_draw(self, control, action, geo_json):
        control.clear_rectangles()

        bbox_geom = geo_json['geometry']['coordinates'][0]

        min_x, min_y = bbox_geom[0]
        max_x, max_y = bbox_geom[2]

        bounds = ((min_y, min_x), (max_y, max_x))
        # self.out.append_display_data(bounds)

        self.map.remove_layer(self.rectangle)
        self.rectangle = Rectangle(
            bounds=bounds,
            color="#1ca45d",
            weight=1
            )
        self.map.add_layer(self.rectangle)

        self.bbox = BBox(((min_x, min_y), (max_x, max_y)), CRS.WGS84).transform(CRS.POP_WEB)

        size_x = abs(int((self.bbox.max_x - self.bbox.min_x) / 10))
        size_y = abs(int((self.bbox.max_y - self.bbox.min_y) / 10))

        self.size = size_x, size_y

    def show(self):
        return self.map
        # return self.vbox
