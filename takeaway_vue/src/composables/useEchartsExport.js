import * as echarts from 'echarts'
import JSZip from 'jszip'
import html2canvas from 'html2canvas'

const html2canvasFn = typeof html2canvas === 'function' ? html2canvas : html2canvas?.default

const dataURLToBlob = (dataURL) => {
  if (!dataURL || typeof dataURL !== 'string') {
    console.warn('[useEchartsExport] invalid dataURL:', dataURL)
    return null
  }

  const parts = dataURL.split(',')
  if (parts.length < 2) {
    console.warn('[useEchartsExport] unexpected dataURL format:', dataURL)
    return null
  }

  const header = parts[0]
  const data = parts[1]
  const match = header.match(/:(.*?);/)
  if (!match) {
    console.warn('[useEchartsExport] unexpected dataURL header:', header)
    return null
  }

  const mime = match[1]
  const binary = atob(data)
  const buffer = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i += 1) {
    buffer[i] = binary.charCodeAt(i)
  }
  return new Blob([buffer], { type: mime })
}

const getEchartsDataURL = (chartDom) => {
  if (!chartDom) return null
  const chart = echarts.getInstanceByDom(chartDom)
  if (!chart) {
    console.warn('[useEchartsExport] no echarts instance found for dom:', chartDom)
    return null
  }
  try {
    return chart.getDataURL({
      type: 'png',
      pixelRatio: 3,
      backgroundColor: '#ffffff',
    })
  } catch (error) {
    console.error('[useEchartsExport] getDataURL failed for dom:', chartDom, error)
    return null
  }
}

const renderWrapperToBlob = async (wrapper) => {
  if (!wrapper) return null
  if (!html2canvasFn) {
    console.warn('[useEchartsExport] html2canvas not available')
    return null
  }

  try {
    const canvas = await html2canvasFn(wrapper, {
      backgroundColor: '#ffffff',
      useCORS: true,
      scale: window.devicePixelRatio || 1,
    })

    return new Promise((resolve) => {
      canvas.toBlob((blob) => {
        if (!blob) {
          console.warn('[useEchartsExport] html2canvas produced null blob for wrapper:', wrapper)
        }
        resolve(blob)
      }, 'image/png')
    })
  } catch (error) {
    console.error('[useEchartsExport] html2canvas capture failed for wrapper:', wrapper, error)
    return null
  }
}

const downloadBlob = (blob, filename) => {
  if (!blob) return
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  link.style.display = 'none'
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}

export default function useEchartsExport(
  chartRefs,
  chartList,
  zipFilename = 'charts.zip',
  fallbackRootSelector = '',
) {
  const findChartDoms = (wrapper) => {
    if (!wrapper) return []
    return Array.from(wrapper.querySelectorAll('.chart'))
  }

  const addChartFiles = async (zip) => {
    for (const item of chartList) {
      const wrapper = chartRefs[item.key]?.value
      if (!wrapper) {
        console.warn('[useEchartsExport] chart wrapper not found for key:', item.key)
        continue
      }

      const chartDoms = findChartDoms(wrapper)
      if (!chartDoms.length) {
        console.warn('[useEchartsExport] no chart doms found inside wrapper for key:', item.key)
      }

      let added = false
      for (const chartDom of chartDoms) {
        const dataURL = getEchartsDataURL(chartDom)
        if (!dataURL) continue

        const blob = dataURLToBlob(dataURL)
        if (blob) {
          zip.file(`${item.name}.png`, blob)
          added = true
        } else {
          console.warn(
            '[useEchartsExport] dataURLToBlob returned null for item:',
            item.name,
            'chartDom:',
            chartDom,
          )
        }
      }

      if (!added) {
        try {
          const blob = await renderWrapperToBlob(wrapper)
          if (blob) {
            zip.file(`${item.name}.png`, blob)
            added = true
          }
        } catch (error) {
          console.error('[useEchartsExport] wrapper export failed for key:', item.key, error)
        }
      }
    }
  }

  const exportAllCharts = async () => {
    console.log('[useEchartsExport] exportAllCharts called')
    try {
      const zip = new JSZip()

      await addChartFiles(zip)
      console.log('[useEchartsExport] files after first pass:', Object.keys(zip.files))

      if (Object.keys(zip.files).length === 0 && fallbackRootSelector) {
        await new Promise((resolve) => setTimeout(resolve, 300))
        await addChartFiles(zip)
        console.log('[useEchartsExport] files after retry:', Object.keys(zip.files))
      }

      if (Object.keys(zip.files).length === 0 && fallbackRootSelector) {
        const chartDoms = Array.from(document.querySelectorAll(`${fallbackRootSelector} .chart`))
        console.warn('[useEchartsExport] fallback search chart count:', chartDoms.length)
        for (let i = 0; i < chartDoms.length; i += 1) {
          const dataURL = getEchartsDataURL(chartDoms[i])
          if (!dataURL) continue
          const blob = dataURLToBlob(dataURL)
          if (blob) {
            zip.file(`chart-${i + 1}.png`, blob)
          } else {
            console.warn('[useEchartsExport] fallback dataURLToBlob returned null for index:', i)
          }
        }
        console.log('[useEchartsExport] files after fallback dom search:', Object.keys(zip.files))

        if (Object.keys(zip.files).length === 0) {
          const wrappers = Array.from(
            document.querySelectorAll(`${fallbackRootSelector} .chart-export-wrapper`),
          )
          console.warn('[useEchartsExport] html2canvas fallback wrapper count:', wrappers.length)
          for (let i = 0; i < wrappers.length; i += 1) {
            try {
              const blob = await renderWrapperToBlob(wrappers[i])
              if (blob) {
                zip.file(`chart-fallback-${i + 1}.png`, blob)
              }
            } catch (error) {
              console.error(
                '[useEchartsExport] html2canvas fallback failed for wrapper:',
                wrappers[i],
                error,
              )
            }
          }
          console.log(
            '[useEchartsExport] files after html2canvas fallback:',
            Object.keys(zip.files),
          )
        }
      }

      if (Object.keys(zip.files).length === 0) {
        console.warn('[useEchartsExport] no chart files found to export')
        alert('未找到可导出的图表，请稍后再试或确认图表是否已渲染完成。')
        return
      }

      const zipBlob = await zip.generateAsync({ type: 'blob' })
      downloadBlob(zipBlob, zipFilename)
    } catch (error) {
      console.error('[useEchartsExport] export failed:', error)
      alert('导出失败，请打开控制台查看错误信息。')
    }
  }

  return { exportAllCharts }
}
