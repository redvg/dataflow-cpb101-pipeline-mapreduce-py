#!/usr/bin/env python


import apache_beam as beam
import argparse

PROJECT_ID = 'udemy-data-engineer-210920'
BUCKET_ID = 'udemy-data-engineer-210920'
BUCKET_FOLDER = 'dataflow-pipeline-py'


def find_matching_lines(line, keyword):

   if line.startswith(keyword):

      yield line

def split_package_name(package_name):

   result = []

   end = package_name.find('.')

   while end > 0:

      result.append(package_name[0:end])

      end = package_name.find('.', end + 1)

   result.append(package_name)

   return result

def resolve_packages(line, keyword):

   start = line.find(keyword) + len(keyword)

   end = line.find(';', start)

   if start < end:

      package_name = line[start:end].strip()

      return split_package_name(package_name)

   return []

def resolve_package_usage(line, keyword):

   packages = resolve_packages(line, keyword)

   for p in packages:

      yield (p, 1)

def compare_by_value(kv1, kv2):

   key1, value1 = kv1

   key2, value2 = kv2

   return value1 < value2

def run():

   argv = [
      '--project={0}'.format(PROJECT_ID),
      '--job_name=verygoodjob',
      '--save_main_session',
      '--staging_location=gs://{0}/{1}/staging/'.format(BUCKET_ID, BUCKET_FOLDER),
      '--temp_location=gs://{0}/{1}/staging/'.format(BUCKET_ID, BUCKET_FOLDER),
      '--runner=DataflowRunner']

   pipeline = beam.Pipeline(argv=argv)

   input = 'gs://{0}/{1}/input/*.java'.format(BUCKET_ID, BUCKET_FOLDER)

   output_prefix = 'gs://{0}/{1}/output'.format(BUCKET_ID, BUCKET_FOLDER)

   keyword = 'import'

   # find most used packages
   (pipeline
      | 'Source' >> beam.io.ReadFromText(input)
      | 'Imports' >> beam.FlatMap(lambda line: find_matching_lines(line, keyword))
      | 'PackageUsage' >> beam.FlatMap(lambda line: resolve_package_usage(line, keyword))
      | 'TotalPackageUse' >> beam.CombinePerKey(sum)
      | 'Top5PackageUsage' >> beam.transforms.combiners.Top.Of(5, compare_by_value)
      | 'Sink' >> beam.io.WriteToText(output_prefix)
   )

   pipeline.run()

if __name__ == '__main__':

    run()
