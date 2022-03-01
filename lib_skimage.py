from skimage import io
from skimage import measure
from skimage import morphology as morph
import numpy as np
import pandas as pd

# Fast way to get the area from an object of type RegionProperties
def fast_area(x) :
    return(np.sum(x._label_image[x._slice] == x.label))

# Process one image
def process_image(source, dest, cfg):
    # read input image
    img = io.imread(source)
    
    if cfg['images']['grayscale'] and len(img.shape) == 3:
        img = img[:,:,0]
    # pdb.set_trace()
    if cfg['images']['crop'] > 0:
        height = img.shape[0]
        img = img[0:(height-cfg['images']['crop']),:]
        # TODO generalise to cropping all sides Cf ecotaxa_ML_template
    
    if cfg['images']['decorner']:
        # white out the corners (very dark parts)
        img = np.where(img < 20, 255, img)

    # invert image (makes it easier to write it afterwards)
    img = 255 - img
    # TODO make this optional or think about what it means

    # threshold it
    img_thresholded = img > cfg['images']['threshold']

    if cfg['images']['dilate_erode'] > 0:
        # dilate and erode to reconnect thin pieces and stay close to the actual border of the object
        img_thresholded = morph.binary_dilation(img_thresholded,
          morph.disk(cfg['images']['dilate_erode']))
        img_thresholded = morph.binary_erosion(img_thresholded,
          morph.disk(cfg['images']['dilate_erode']))

    # detect particles
    img_labelled = measure.label(img_thresholded, background=False, connectivity=2)

    # measure particles and keep only the largest one
    regions = measure.regionprops(label_image=img_labelled, intensity_image=img)
    areas = [fast_area(r) for r in regions]
    particle_label = np.argmax(areas) + 1

    # properties to compute and save
    prop_names = ['area',
                  'convex_area',
                  'eccentricity',
                  'equivalent_diameter',
                  'euler_number',
                  'filled_area',
                  'inertia_tensor',
                  'inertia_tensor_eigvals',
                  'major_axis_length',
                  'max_intensity',
                  'mean_intensity',
                  'min_intensity',
                  'minor_axis_length',
                  'moments_hu',
                  'moments_normalized',
                  'perimeter',
                  'solidity',
                  'weighted_moments_hu',
                  'weighted_moments_normalized']


    # measure the largest particle
    largest_props = measure.regionprops_table(
        label_image=(img_labelled==particle_label).astype(np.uint8),
        intensity_image=img,
        properties=prop_names
    )
    # largest_props['objid'] = df.index[i]

    largest_props = pd.DataFrame(largest_props)
    # features = features.append(largest_props, verify_integrity=True)
    # NB: prevent duplicate indexes

    # extract the particle
    part = regions[particle_label-1].intensity_image
    # NB: the non particle region is marked black
    # re-invert it
    part = 255 - part
    # and save it as an image
    io.imsave(fname=dest, arr=part)
    
    return(largest_props)

# Process several images defined in a DataFrame
def process_images(df, cfg):
  props = [process_image(df['source'].values[i], df['dest'].values[i], cfg) for i in range(df.shape[0])]
  return(pd.concat(props, ignore_index=True))
