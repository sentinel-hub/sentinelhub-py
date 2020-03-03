from ipyleaflet import Map, DrawControl, Marker, Rectangle
from sentinelhub import BBox, CRS

from ipywidgets import widgets as w


class BBoxSelector:
    def __init__(self, bbox, zoom=8, resolution=10):
        center = (bbox.min_y + bbox.max_y) / 2, (bbox.min_x + bbox.max_x) / 2
        self.map = Map(center=center, zoom=zoom, scroll_wheel_zoom=True)

        self.resolution = resolution

        control = DrawControl()

        control.rectangle = {
            "shapeOptions": {
                "fillColor": "#fabd14",
                "color": "#fa6814",
                "fillOpacity": 0.2
            }
        }

        #Disable the rest of draw options
        control.polyline = {}
        control.circle = {}
        control.circlemarker = {}
        control.polygon = {}
        control.edit = False
        control.remove = False

        control.on_draw(self._handle_draw)

        self.map.add_control(control)

        self.bbox = None
        self.size = None
        self.rectangle = None
        self.add_rectangle(bbox.min_x, bbox.min_y, bbox.max_x, bbox.max_y)

        # self.out = w.Output(layout=w.Layout(width='100%', height='50px', overflow_y='scroll'))
        # self.vbox = w.VBox([self.map, self.out])

    def add_rectangle(self, min_x, min_y, max_x, max_y):
        if self.rectangle:
            self.map.remove_layer(self.rectangle)

        self.rectangle = Rectangle(
            bounds=((min_y, min_x), (max_y, max_x)),
            color="#fa6814",
            fill=True,
            fill_color="#fabd14",
            fill_opacity=0.2,
            weight=1
        )

        self.map.add_layer(self.rectangle)

        self.bbox = BBox(((min_x, min_y), (max_x, max_y)), CRS.WGS84).transform(CRS.POP_WEB)

        # self.out.append_display_data((min_x, min_y, max_x, max_y))

        size_x = abs(int((self.bbox.max_x - self.bbox.min_x) / self.resolution))
        size_y = abs(int((self.bbox.max_y - self.bbox.min_y) / self.resolution))

        self.size = size_x, size_y

    def _handle_draw(self, control, action, geo_json):
        control.clear_rectangles()

        bbox_geom = geo_json['geometry']['coordinates'][0]

        min_x, min_y = bbox_geom[0]
        max_x, max_y = bbox_geom[2]

        self.add_rectangle(min_x, min_y, max_x, max_y)

    def show(self):
        return self.map
        # return self.vbox
