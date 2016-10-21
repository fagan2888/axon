import matplotlib.dates as md
import matplotlib.pyplot as plt
import numpy as np

import vibepy.geo_spatial_func_lib as geosp

try:
    from ..traineval.avg_fun import define_outlier_threshold
except:
    from traineval.avg_fun import define_outlier_threshold



def display_trips(trip_info, itin_info, legs_info, pts_info, CONFIG):

    # Decide if saving pictures as images or plots
    # Lots of reset index / set index -- Needs optimization
    # These joins could be better done in postgres ?? before loading
    # skip = True
    for mot_segment_id, trip in trip_info.sort_values(['vid', 'time_start']).iterrows():

        # if trip['vid'] != 'e03838d0-a52a-4ea8-b765-30bdaff6b713': continue
        # if mot_segment_id != 'caeda309-5e6b-42fe-8ee3-6f543ed0be07': continue
        # if mot_segment_id not in (''
        #                           ):
        #     continue


        # if trip['vid'][:5] != 'AVAND': continue
        #
        # if mot_segment_id == '2b76b07e-07c0-4902-89b9-3a848242a01c':
        #     skip = False
        # if skip: continue

        print '\n\nMoT segment ID: ', mot_segment_id

        print trip['warning_str']
        # if not trip['warning_bool']:
        #     continue

        itin_subset = itin_info[itin_info['mot_segment_id'] == mot_segment_id].sort_values(['warning_bool',
                                                                                            'kde_weighed_avg'])
        legs_subset = legs_info[legs_info['mot_segment_id'] == mot_segment_id]
        pts_subset = pts_info[pts_info['mot_segment_id'] == mot_segment_id].sort_values('time')

        display_single_trip(trip, itin_subset, legs_subset, pts_subset, CONFIG)

    return


def display_single_trip(trip, itin_subset, legs_subset, pts_subset, CONFIG):

    mapbox_api_url = CONFIG.get('mapbox', 'API_URL')

    # TODO find a better name than 'u'
    u = geosp.MapImg(img_size=(650, 650))
    u.set_map_api_url(mapbox_api_url)

    u.find_optimal_zoom(pts_subset, col_names=('lat', 'lon'))
    map_img = u.get_map_img()

    dpi = 80
    figsize = 4 * u.img_width / dpi, 2 * u.img_height / dpi
    plt.figure(figsize=figsize, dpi=dpi)

    # Start with the plots - maps disappear otherwise...
    plt.subplot(222)
    plot_schedules(trip, itin_subset, legs_subset, pts_subset)

    pts = pts_subset.loc[pts_subset['itinerary_id'] == trip['itinerary_id'],
                         ['time', 'distance', 'ooo_outlier', 'horizontal_accuracy', 'gh_outlier']]
    best_itin_legs = legs_subset[legs_subset['itinerary_id'] == itin_subset.index[0]]
    if pts['distance'].max() > 16000:
        plt.subplot(428)
        plot_distance_ts(pts, itin_subset, best_itin_legs, pos='bottom')
        plt.subplot(426)
        plot_distance_ts(pts, itin_subset, best_itin_legs, pos='top')
    else:
        plt.subplot(224)
        plot_distance_ts(pts, itin_subset, best_itin_legs)

    # RAW DATA (all data)
    ax = plt.subplot(241)
    plot_map_raw(pts_subset, map_img, u, ax)
    plot_start_end_markers(u, trip, ax)

    # Selected Itinerary
    ax = plt.subplot(242)
    plot_map_best_itin(pts_subset, map_img, u, trip['itinerary_id'], ax)
    plot_start_end_markers(u, trip, ax)

    # The two lower maps are zoom in on start/end locations
    u.set_zoom(int(CONFIG.get('mapbox', 'START_END_ZOOM_LEVEL')))

    ax = plt.subplot(245)
    u.set_img_center_position((trip['lat_start'], trip['lon_start']))
    map_img = u.get_map_img()
    plot_map_zoom(pts_subset, map_img, u, trip['itinerary_id'], ax)
    plot_start_end_markers(u, trip, ax)
    plt.title('Departure (Blue = Within best Itinerary)')

    ax = plt.subplot(246)
    u.set_img_center_position((trip['lat_end'], trip['lon_end']))
    map_img = u.get_map_img()
    plot_map_zoom(pts_subset, map_img, u, trip['itinerary_id'], ax)
    plot_start_end_markers(u, trip, ax)
    plt.title('Arrival (Blue = Within best Itinerary)')

    plt.tight_layout()
    plt.show()
    # fname = '/Users/simongelinas/Documents/Output/sbb_SM_warning/{f}.png'.format(f=trip.name)
    # plt.savefig(fname, bbox_inches='tight')
    plt.close('all')


    return


def plot_map_raw(pts_subset, map_img, u, ax):

    unique_pts = pts_subset[['point_id', 'lat', 'lon', 'horizontal_accuracy', 'within_mot_segment']].drop_duplicates()
    x, y = u.project_to_px(unique_pts['lat'].values, unique_pts['lon'].values)
    mask = (unique_pts['within_mot_segment'] == False).values

    plot_map(x, y, mask, map_img, u, mask_line=False, ax=ax)
    plt.title('All Raw Data (Blue = Within MoT)')

    return


def plot_map_best_itin(points_data, map_img, u, itin_id, ax):

    pts = points_data[points_data['itinerary_id'] == itin_id].drop_duplicates()
    x, y = u.project_to_px(pts['lat'].values, pts['lon'].values)
    mask = (pts['ooo_outlier'] == True).values

    plot_map(x, y, mask, map_img, u, mask_line=True, ax=ax)
    plt.title('Selected Itinerary (Blue = Not Outlier)')

    return


def plot_map_zoom(points_data, map_img, u, itin_id, ax):

    pts = points_data[['point_id', 'lat', 'lon', 'horizontal_accuracy', 'itinerary_id']].drop_duplicates()
    x, y = u.project_to_px(pts['lat'].values, pts['lon'].values)
    mask = (pts['itinerary_id'] != itin_id).values

    plot_map(x, y, mask, map_img, u, mask_line=False, ax=ax)

    return


def plot_map(x, y, mask, map_img, u, mask_line=False, ax=None):
    """
    When mask=True they get flagged in RED i.e. problematic points
    """

    if not ax:
        ax = plt.gca()

    ax.imshow(map_img)  # , interpolation='none')

    ax.scatter(x[mask], y[mask], marker='o', color=(1., .4, .4), s=26, alpha=0.6)
    ax.scatter(x[~mask], y[~mask], marker='s', color=(.4, .4, 1.), s=22, alpha=0.8)

    if mask_line:
        ax.plot(x[~mask], y[~mask], linestyle='-', color=(0.2, 1, 0.4), lw=5, alpha=0.4)
    else:
        ax.plot(x, y, linestyle='-', color=(0.2, 1, 0.4), lw=3, alpha=0.3)

    plt.xlim(0, u.img_width)
    plt.ylim(u.img_height, 0)
    plt.axis('off')

    return


def plot_start_end_markers(u, coord_se, ax):
    start = u.project_to_px(coord_se['lat_start'], coord_se['lon_start'])
    ax.scatter(start[0], start[1], color='r', marker='x', s=42)

    end = u.project_to_px(coord_se['lat_end'], coord_se['lon_end'])
    ax.scatter(end[0], end[1], color='r', marker='*', s=38)
    return


def plot_distance_ts(distance_ts, itin_subset, legs_best_itin, time_buffer=0.1, pos=None):

    # To render only Hours:Minutes on the horizontal axis
    ax = plt.gca()
    xfmt = md.DateFormatter('%H:%M')
    ax.xaxis.set_major_formatter(xfmt)

    # Show best itinerary legs
    for leg_idx, row in legs_best_itin.iterrows():
        plt.plot([row['time_start'], row['time_end']], [0, 0], lw=12, c=(0.8, 0.9, 0.85))

    # Adds a buffer% buffer on the right/left of the time axis
    time_delta_buffer = time_buffer * (distance_ts['time'].max() - distance_ts['time'].min())

    # Plot the time series of data / outliers
    mask = (distance_ts['ooo_outlier'] == True).values
    mask_gh = distance_ts['gh_outlier'] == True
    ax.errorbar(distance_ts.loc[~mask, 'time'], distance_ts.loc[~mask, 'distance'],
                yerr=distance_ts.loc[~mask, 'horizontal_accuracy'], fmt='.', lw=2, ecolor=(0.5, 0.7, 1.), capthick=0)
    plt.plot(distance_ts.loc[~mask, 'time'], distance_ts.loc[~mask, 'distance'], linestyle='--', marker='s', c='b')
    plt.plot(distance_ts.loc[mask, 'time'], distance_ts.loc[mask, 'distance'], 'ro')
    plt.plot(distance_ts.loc[mask_gh, 'time'], distance_ts.loc[mask_gh, 'distance'], linestyle='', marker='.', c='w')


    # plot outlier threshold
    x_thr = [distance_ts.loc[~mask, 'time'].min(),  distance_ts.loc[~mask, 'time'].max()]
    y_thr = [define_outlier_threshold(distance_ts.loc[~mask, 'distance'])] * 2
    plt.plot(x_thr, y_thr, c='r', linestyle=':', lw=2)

    # Plot the itinerary kde-averaged values on the left-hand side of the graph
    avg_x0 = (distance_ts['time'].min() - time_delta_buffer*0.8)
    x0 = [avg_x0 for i in range(itin_subset.shape[0])]
    plt.scatter(x0, itin_subset['kde_weighed_avg'], s=200, marker='<', facecolors='w', edgecolors=(0., 0., 0.1), lw=2)

    # Visual changes (axis labels / limits)
    plt.xlim(distance_ts['time'].min() - time_delta_buffer, distance_ts['time'].max() + time_delta_buffer)

    if pos == 'bottom':
        plt.ylabel('Distance (m, <8k)')
        plt.ylim((-100, 8000))
    elif pos == 'top':
        plt.ylabel('Distance (m, >8k)')
        plt.ylim((8000, max(16000, distance_ts['distance'].max()*1.1)))  # ensures min of 16k to avoid scaling effects
        plt.gca().get_xaxis().set_visible(False)
    else:
        plt.ylabel('Distance (m)')
        plt.ylim((-100, 16000))

    if pos != 'top':
        plt.xlabel('Time')

    return


def plot_schedules(trip, itin_subset, legs_subset, pts_subset, time_buffer=0.3):

    # To render only Hours:Minutes on the horizontal axis
    ax = plt.gca()
    xfmt = md.DateFormatter('%H:%M')
    ax.xaxis.set_major_formatter(xfmt)

    colors = {True: (1., 0.9, 0.9), False: (0.8, 0.9, 0.85)}

    plt.plot([trip['time_start'], trip['time_end']], [0.2, 0.2], lw=8, c=(0.9, 0.9, 0.9))

    y0 = 1
    for itin_idx, itin_row in itin_subset.iterrows():
        for leg_idx, row in legs_subset[legs_subset['itinerary_id'] == itin_idx].iterrows():
            plt.plot([row['time_start'], row['time_end']], [y0, y0], lw=12, c=colors[itin_row['warning_bool']])
        y0 += 1

    # Distance from start / end station over time (overlays the schedules)

    gsc = geosp.GeoSpConv()  # geospatial conversion object

    pts = pts_subset[['point_id', 'lat', 'lon', 'time', 'within_mot_segment', 'gh_outlier']].drop_duplicates().copy()
    pts['y'] = pts['lat'].apply(gsc.lat2met)
    pts['x'] = pts['lon'].apply(gsc.lon2met)

    # Conversion to meters
    lat_start = gsc.lat2met(trip['lat_start'])
    lon_start = gsc.lon2met(trip['lon_start'])
    lat_end = gsc.lat2met(trip['lat_end'])
    lon_end = gsc.lon2met(trip['lon_end'])

    dist_tot = dist_from_pt(lat_start, lon_start, (lat_end, lon_end))
    dist_from_start = dist_from_pt(pts['y'], pts['x'], (lat_start, lon_start), dist_tot)
    dist_from_end = dist_from_pt(pts['y'], pts['x'], (lat_end, lon_end), dist_tot)

    # plt.plot(pts['time'], 10 * dist_from_start, color=(0.9, 0.3, 0.1), lw=1.5, marker='.', markersize=12, linestyle='--')
    # plt.plot(pts['time'], 10 * dist_from_end, color=(0.1, 0.3, 0.9), lw=1.5, marker='.', markersize=12, linestyle='--')
    gh_mask = pts['gh_outlier'] == True
    fpga_mask = pts['point_id'] >= 0

    if (~fpga_mask).astype(int).sum() > 0:
        plt.plot(pts.loc[~fpga_mask, 'time'], 10 * dist_from_start.loc[~fpga_mask], color='k', marker='*', markersize=8, linestyle='')
        plt.plot(pts.loc[~fpga_mask, 'time'], 10 * dist_from_end.loc[~fpga_mask], color='k', marker='*', markersize=8, linestyle='')
    if (fpga_mask).astype(int).sum() > 0:
        plt.plot(pts.loc[fpga_mask, 'time'], 10 * dist_from_start.loc[fpga_mask], color=(0.9, 0.3, 0.1), lw=1.5, marker='.', markersize=12, linestyle='--')
        plt.plot(pts.loc[fpga_mask, 'time'], 10 * dist_from_end.loc[fpga_mask], color=(0.1, 0.3, 0.9), lw=1.5, marker='.', markersize=12, linestyle='--')
    if gh_mask.astype(int).sum() > 0:
        plt.plot(pts.loc[gh_mask, 'time'], 10 * dist_from_start.loc[gh_mask], color='w', marker='.', markersize=6, linestyle='')
        plt.plot(pts.loc[gh_mask, 'time'], 10 * dist_from_end.loc[gh_mask], color='w', marker='.', markersize=6, linestyle='')

    # Adds a 10% buffer on the right/left of the time axis
    time_delta_buffer = time_buffer * (pts['time'].max() - pts['time'].min())
    plt.xlim(pts['time'].min() - time_delta_buffer, pts['time'].max() + time_delta_buffer)

    if np.any(dist_from_start > 2) or np.any(dist_from_end > 2):
        plt.ylim((0, 20))

    # Text box
    info_str = format_itinerary_output(trip, legs_subset[legs_subset['itinerary_id'] == itin_subset.index[0]])
    # ax.text doesn't support non ascii characters apaprently...
    ax.text(.05, .80, remove_non_ascii(info_str), horizontalalignment='left', transform=ax.transAxes)

    return


def remove_non_ascii(s): return "".join(i for i in s if ord(i) < 128)
# from:  http://stackoverflow.com/questions/1342000/
#        how-to-make-the-python-interpreter-correctly-handle-non-ascii-characters-in-stri


def format_itinerary_output(trip, legs_best_itin):
    """ Generates a nicely formatted string that incorporates all legs of an itinerary
    """

    schedule_items_list = []
    schedule_item = '   {start_name} - {start_time} to {end_name} - {end_time} ({route_name})'
    for idx, leg in legs_best_itin.iterrows():

        if not schedule_items_list:
            schedule_items_list.append(leg['time_start'].strftime('%A %d. %B %Y'))

        values = {
            'start_name': leg['station_name_start'],
            'start_time': leg['time_start'].strftime('%H:%M'),
            'end_name': leg['station_name_end'],
            'end_time': leg['time_end'].strftime('%H:%M'),
            'route_name': leg['route_full_name']
        }
        schedule_items_list.append(schedule_item.format(**values))

    schedule_items_list.append('\nConfidence: {c} %'.format(c=int(np.round(trip['confidence']*100))))
    schedule_items_list.append('{i}'.format(i=trip.name))

    schedule_str = '\n'.join(schedule_items_list)
    return schedule_str


def dist_from_pt(lat_array, lon_array, ref_pt, norm_fact=1.):
    return np.sqrt(np.power(lon_array - ref_pt[1], 2) + np.power(lat_array - ref_pt[0], 2)) / norm_fact


def print_pts_geojson(pts, style='MultiPoint'):

    # convert to string with 7 digits after the point in lon,lat notation
    x1 = np.round(pts[['lat', 'lon']], 7).astype(str)
    # Build the concatenated string
    concat_str = ','.join(('[' + x1['lon'] + ',' + x1['lat'] + ']').tolist())
    header = '{{"type": "{s}", "coordinates": ['.format(s=style)
    return header+concat_str+']}'

# def test_ax_fun(nbym=(1,1), dpi=80, margin=0.0, xpixels=800, ypixels=800):
#     # This was really clever but still doesn't work because of stupid matplotlib
#
#     # http://stackoverflow.com/questions/8056458/display-image-with-a-zoom-1-with-matplotlib-imshow-how-to
#
#     # Make a figure big enough to accomodate an axis of xpixels by ypixels
#     # as well as the ticklabels, etc...
#     figsize = nbym[0]*(1 + margin) * xpixels / dpi, nbym[1]*(1 + margin) * ypixels / dpi
#     fig = plt.figure(figsize=figsize, dpi=dpi)
#
#     # Make the axis the right size...
#     ax = []
#     unit_width = 1. / nbym[0]
#     unit_height = 1. / nbym[1]
#     # [left, bottom, width, height] values in 0-1 relative figure coordinates:
#     for i in range(nbym[0]):
#         for j in range(nbym[1]):
#             # new_ax = fig.add_axes([margin, margin, 1 - 2 * margin, 1 - 2 * margin])
#             new_ax = fig.add_axes([i*unit_width+margin,
#                                    j*unit_height+margin,
#                                    unit_width - 2 * margin,
#                                    unit_height - 2 * margin])
#             # new_ax.imshow(a, interpolation='none')
#             plt.axis('off')
#             ax.append(new_ax)
#
#     return fig, ax
