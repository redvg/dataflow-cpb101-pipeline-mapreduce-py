#!/usr/bin/env python


import apache_beam as beam
import argparse

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

   parser = argparse.ArgumentParser(description='Find the most used Java packages')

   parser.add_argument('--output_prefix',
                       default='/tmp/output',
                       help='Output prefix')

   parser.add_argument('--input',
                       default='../javahelp/src/main/java/com/google/cloud/training/dataanalyst/javahelp/',
                       help='Input files location directory')

   options, pipeline_args = parser.parse_known_args()

   pipeline = beam.Pipeline(argv=pipeline_args)

   input = '{0}*.java'.format(options.input)

   output_prefix = options.output_prefix

   keyword = 'import'

   # find most used packages
   (pipeline
      | 'GetInput' >> beam.io.ReadFromText(input)
      | 'Imports' >> beam.FlatMap(lambda line: find_matching_lines(line, keyword))
      | 'PackageUsage' >> beam.FlatMap(lambda line: resolve_package_usage(line, keyword))
      | 'TotalPackageUse' >> beam.CombinePerKey(sum)
      | 'Top5PackageUsage' >> beam.transforms.combiners.Top.Of(5, compare_by_value)
      | 'WriteOutput' >> beam.io.WriteToText(output_prefix)
   )

   pipeline.run().wait_until_finish()    

if __name__ == '__main__':

    run()
