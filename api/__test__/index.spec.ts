import test from 'ava'

import { exportParsedStandardDirectory } from '../index'

test('exportParsedStandardDirectory should create unique dirs and copy instances in order', async (t) => {
  // const exportRootDir ='/Users/dxw/Downloads/dicom/export-test'
  const exportRootDir ='c:/Users/dxw/Downloads/dicom/export-test'

  const parsedDirectoryJson = {
    PatientName: '123',
    studies: {
      '1111.2222.3333.4445.20250614000360': {
        StudyDescription: '1',
        series: {
          '1.2.392.200046.100.14.5936173071165728854242105939978607325970': {
            SeriesDescription: 'Unknown Series',
            SeriesNumber: 1,
            instances: {
              '1.2.392.200046.100.14.191336221617017027080648845861400719769': {
                fileName: '1.DCM',
                filePath: 'c:/Users/dxw/Downloads/dicom/me/1.DCM',
              },
            },
            instancesInOrder: [{ key: '1.2.392.200046.100.14.191336221617017027080648845861400719769' }],
          }
        },
        seriesInOrder: [{ key: '1.2.392.200046.100.14.5936173071165728854242105939978607325970' }],
      }
    },
    studiesInOrder: [{ key: '1111.2222.3333.4445.20250614000360' }],
  }

  // exportParsedStandardDirectory(
  //   JSON.stringify(parsedDirectoryJson),
  //   exportRootDir,
  //   0,
  // )

  // exportParsedStandardDirectory(
  //   JSON.stringify(parsedDirectoryJson),
  //   exportRootDir,
  //   1,
  // )

  // exportParsedStandardDirectory(
  //   JSON.stringify(parsedDirectoryJson),
  //   exportRootDir,
  //   2,
  // )

  exportParsedStandardDirectory(
    JSON.stringify(parsedDirectoryJson),
    exportRootDir,
    300,
  )

  t.true(true)
})