"""Formats

This module handles creation of output files in GeoTIF, netCDF, and other image formats.
"""

import numpy
import subprocess
import os
from osgeo import gdal, osr
from netCDF4 import Dataset
from matplotlib import cm, pyplot as plt
from PIL import Image


def compress_geotiff(input_file):
    temp_out = input_file.replace(".tif", "_compress.tif")
    subprocess.call(["gdal_translate", "-co", "COMPRESS=LZW", input_file, temp_out])
    if os.path.isfile(temp_out):
        os.remove(input_file)
        os.rename(temp_out, input_file)


def create_geotiff(pixels, gps_bounds, out_path, nodata=-99, asfloat=False, extractor_info=None, system_md=None, extra_metadata=None, compress=False):
    """Generate output GeoTIFF file given a numpy pixel array and GPS boundary.

        Keyword arguments:
        pixels -- numpy array of pixel values.
                    if 2-dimensional array, a single-band GeoTIFF will be created.
                    if 3-dimensional array, a band will be created for each Z dimension.
        gps_bounds -- tuple of GeoTIFF coordinates as ( lat (y) min, lat (y) max,
                                                        long (x) min, long (x) max)
        out_path -- path to GeoTIFF to be created
        nodata -- NoDataValue to be assigned to raster bands; set to None to ignore
        float -- whether to use GDT_Float32 data type instead of GDT_Byte (e.g. for decimal numbers)
        extractor_info -- details about extractor if applicable
        system_md -- cleaned TERRA-REF metadata
        extra_metadata -- any metadata to be embedded in geotiff; supersedes extractor_info and system_md
    """
    dimensions = numpy.shape(pixels)
    if len(dimensions) == 2:
        nrows, ncols = dimensions
        channels = 1
    else:
        nrows, ncols, channels = dimensions

    geotransform = (
        gps_bounds[2], # upper-left x
        (gps_bounds[3] - gps_bounds[2])/float(ncols), # W-E pixel resolution
        0, # rotation (0 = North is up)
        gps_bounds[1], # upper-left y
        0, # rotation (0 = North is up)
        -((gps_bounds[1] - gps_bounds[0])/float(nrows)) # N-S pixel resolution
    )

    # Create output GeoTIFF and set coordinates & projection
    if asfloat:
        dtype = gdal.GDT_Float32
    else:
        dtype = gdal.GDT_Byte

    if compress:
        output_raster = gdal.GetDriverByName('GTiff') \
            .Create(out_path, ncols, nrows, channels, dtype, ['COMPRESS=LZW'])
    else:
        output_raster = gdal.GetDriverByName('GTiff') \
            .Create(out_path, ncols, nrows, channels, dtype)

    output_raster.SetGeoTransform(geotransform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326) # google mercator
    output_raster.SetProjection( srs.ExportToWkt() )

    if not extra_metadata:
        extra_metadata = prepare_metadata_for_geotiff(extractor_info, system_md)

    output_raster.SetMetadata(extra_metadata)

    if channels == 3:
        # typically 3 channels = RGB channels
        # TODO: Something wonky w/ uint8s --> ending up w/ lots of gaps in data (white pixels)
        output_raster.GetRasterBand(1).WriteArray(pixels[:,:,0].astype('uint8'))
        output_raster.GetRasterBand(1).SetColorInterpretation(gdal.GCI_RedBand)
        output_raster.GetRasterBand(1).FlushCache()
        if nodata:
            output_raster.GetRasterBand(1).SetNoDataValue(nodata)

        output_raster.GetRasterBand(2).WriteArray(pixels[:,:,1].astype('uint8'))
        output_raster.GetRasterBand(2).SetColorInterpretation(gdal.GCI_GreenBand)
        output_raster.GetRasterBand(2).FlushCache()
        if nodata:
            output_raster.GetRasterBand(2).SetNoDataValue(nodata)

        output_raster.GetRasterBand(3).WriteArray(pixels[:,:,2].astype('uint8'))
        output_raster.GetRasterBand(3).SetColorInterpretation(gdal.GCI_BlueBand)
        output_raster.GetRasterBand(3).FlushCache()
        if nodata:
            output_raster.GetRasterBand(3).SetNoDataValue(nodata)

    elif channels > 1:
        # TODO: Something wonky w/ uint8s --> ending up w/ lots of gaps in data (white pixels)
        for chan in range(channels):
            band = chan + 1
            output_raster.GetRasterBand(band).WriteArray(pixels[:,:,chan].astype('uint8'))
            output_raster.GetRasterBand(band).FlushCache()
            if nodata:
                output_raster.GetRasterBand(band).SetNoDataValue(nodata)
    else:
        # single channel image, e.g. temperature
        output_raster.GetRasterBand(1).WriteArray(pixels)
        output_raster.GetRasterBand(1).FlushCache()
        if nodata:
            output_raster.GetRasterBand(1).SetNoDataValue(nodata)

    output_raster = None


def prepare_metadata_for_geotiff(extractor_info=None, terra_md=None):
    """Create geotiff-embedded metadata from extractor_info and terraref metadata pieces.

        Keyword arguments:
        extractor_info -- details about extractor if applicable
        system_md -- cleaned TERRA-REF metadata
    """
    extra_metadata = {}

    if (terra_md != None):
        extra_metadata["datetime"] = str(terra_md["gantry_variable_metadata"]["datetime"])
        extra_metadata["sensor_id"] = str(terra_md["sensor_fixed_metadata"]["sensor_id"])
        extra_metadata["sensor_url"] = str(terra_md["sensor_fixed_metadata"]["url"])
        experiment_names = []
        for e in terra_md["experiment_metadata"]:
            experiment_names.append(e["name"])
        terra_md["experiment_name"] = ", ".join(experiment_names)

    if (extractor_info != None):
        extra_metadata["extractor_name"] = str(extractor_info.get("name", ""))
        extra_metadata["extractor_version"] = str(extractor_info.get("version", ""))
        extra_metadata["extractor_author"] = str(extractor_info.get("author", ""))
        extra_metadata["extractor_description"] = str(extractor_info.get("description", ""))
        if "repository" in extractor_info and "repUrl" in extractor_info["repository"]:
            extra_metadata["extractor_repo"] = str(extractor_info["repository"]["repUrl"])
        else:
            extra_metadata["extractor_repo"] = ""

    return extra_metadata


def create_netcdf(pixels, out_path, scaled=False):
    """Generate output netCDF file given an input numpy pixel array.

            Keyword arguments:
            pixels -- 2-dimensional numpy array of pixel values
            out_path -- path to GeoTIFF to be created
            scaled -- whether to scale PNG output values based on pixels min/max
        """
    dimensions = numpy.shape(pixels)
    if len(dimensions) == 2:
        nrows, ncols = dimensions
        channels = 1
    else:
        nrows, ncols, channels = dimensions

    out_nc = Dataset(out_path, "w", format="NETCDF4")
    out_nc.createDimension('band',  channels) # only 1 band for mask
    out_nc.createDimension('x', ncols)
    out_nc.createDimension('y', nrows)

    mask = out_nc.createVariable('soil_mask','f8',('band', 'x', 'y'))
    mask[:] = pixels

    out_nc.close()


def create_image(pixels, out_path, scaled=False):
    """Generate output JPG/PNG file given an input numpy pixel array.

            Keyword arguments:
            pixels -- 2-dimensional numpy array of pixel values
            out_path -- path to GeoTIFF to be created
            scaled -- whether to scale PNG output values based on pixels min/max
        """
    if scaled:
        # e.g. flirIrCamera
        Gmin = pixels[pixels>0].min()
        Gmax = pixels.max()
        scaled_px = (pixels-Gmin)/(Gmax - Gmin)
        plt.imsave(out_path, cm.get_cmap('jet')(scaled_px))
    else:
        # e.g. PSII
        # TODO: Can we make these use same library?
        # TODO: plt.imsave(out_path, pixels)
        Image.fromarray(pixels).save(out_path)
