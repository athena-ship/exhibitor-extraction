import { useState, useEffect, useCallback } from 'react'
import { 
  RefreshCw, 
  Play, 
  CheckCircle, 
  XCircle, 
  Loader2,
  ExternalLink,
  Calendar,
  MapPin,
  Building
} from 'lucide-react'

interface PendingRequest {
  row_index: number
  date_requested: string | null
  show_name: string | null
  start_date: string | null
  end_date: string | null
  location: string | null
  floorplan_url: string | null
  exhibitor_list_url: string | null
  delivered: string | null
  exhibitors: number | null
  large_booths: number | null
  missing_contact_info: number | null
}

interface JobResult {
  show_name: string
  status: string
  exhibitor_count?: number
  file_link?: string
  error?: string
}

interface JobStatus {
  job_id: string
  status: 'pending' | 'processing' | 'completed' | 'failed'
  progress: number
  message: string
  results: JobResult[] | null
  error: string | null
}

function App() {
  const [pendingRequests, setPendingRequests] = useState<PendingRequest[]>([])
  const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set())
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null)
  const [polling, setPolling] = useState(false)

  const fetchPendingRequests = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch('/api/pending')
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }
      const data = await response.json()
      setPendingRequests(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch pending requests')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchPendingRequests()
  }, [fetchPendingRequests])

  // Poll for job status updates
  useEffect(() => {
    if (!polling || !jobStatus) return

    const poll = async () => {
      try {
        const response = await fetch(`/api/status/${jobStatus.job_id}`)
        if (response.ok) {
          const status: JobStatus = await response.json()
          setJobStatus(status)
          
          if (status.status === 'completed' || status.status === 'failed') {
            setPolling(false)
            // Refresh pending requests after completion
            if (status.status === 'completed') {
              await fetchPendingRequests()
            }
          }
        }
      } catch (err) {
        console.error('Failed to poll job status:', err)
      }
    }

    const interval = setInterval(poll, 2000)
    return () => clearInterval(interval)
  }, [polling, jobStatus, fetchPendingRequests])

  const toggleRow = (rowIndex: number) => {
    const newSelected = new Set(selectedRows)
    if (newSelected.has(rowIndex)) {
      newSelected.delete(rowIndex)
    } else {
      newSelected.add(rowIndex)
    }
    setSelectedRows(newSelected)
  }

  const selectAll = () => {
    if (selectedRows.size === pendingRequests.length) {
      setSelectedRows(new Set())
    } else {
      setSelectedRows(new Set(pendingRequests.map(r => r.row_index)))
    }
  }

  const processSelected = async () => {
    if (selectedRows.size === 0) return

    try {
      const response = await fetch('/api/process', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_indices: Array.from(selectedRows) })
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const data = await response.json()
      setJobStatus({
        job_id: data.job_id,
        status: 'pending',
        progress: 0,
        message: 'Starting...',
        results: null,
        error: null
      })
      setPolling(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start processing')
    }
  }

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return '-'
    return dateStr
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">
                Exhibitor Extraction
              </h1>
              <p className="text-sm text-gray-500 mt-1">
                Absolute Exhibits - Trade Show Lead Generation
              </p>
            </div>
            <button
              onClick={fetchPendingRequests}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Error Alert */}
        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg flex items-center gap-3">
            <XCircle className="w-5 h-5 text-red-500" />
            <span className="text-red-700">{error}</span>
            <button
              onClick={() => setError(null)}
              className="ml-auto text-red-500 hover:text-red-700"
            >
              ×
            </button>
          </div>
        )}

        {/* Job Status */}
        {jobStatus && (
          <div className="mb-6 p-4 bg-white border border-gray-200 rounded-lg shadow-sm">
            <div className="flex items-center justify-between mb-3">
              <h3 className="font-medium text-gray-900">Processing Status</h3>
              <span className={`px-2 py-1 text-xs font-medium rounded-full ${
                jobStatus.status === 'completed' ? 'bg-green-100 text-green-700' :
                jobStatus.status === 'failed' ? 'bg-red-100 text-red-700' :
                jobStatus.status === 'processing' ? 'bg-blue-100 text-blue-700' :
                'bg-gray-100 text-gray-700'
              }`}>
                {jobStatus.status.charAt(0).toUpperCase() + jobStatus.status.slice(1)}
              </span>
            </div>
            
            <div className="mb-3">
              <div className="flex justify-between text-sm text-gray-600 mb-1">
                <span>{jobStatus.message}</span>
                <span>{jobStatus.progress}%</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div
                  className={`h-2 rounded-full transition-all ${
                    jobStatus.status === 'failed' ? 'bg-red-500' :
                    jobStatus.status === 'completed' ? 'bg-green-500' :
                    'bg-blue-500'
                  }`}
                  style={{ width: `${jobStatus.progress}%` }}
                />
              </div>
            </div>

            {jobStatus.results && (
              <div className="mt-4 space-y-2">
                {jobStatus.results.map((result, idx) => (
                  <div
                    key={idx}
                    className={`p-3 rounded-lg ${
                      result.status === 'completed' ? 'bg-green-50' : 'bg-red-50'
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {result.status === 'completed' ? (
                          <CheckCircle className="w-5 h-5 text-green-500" />
                        ) : (
                          <XCircle className="w-5 h-5 text-red-500" />
                        )}
                        <span className="font-medium">{result.show_name}</span>
                      </div>
                      {result.file_link && (
                        <a
                          href={result.file_link}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="flex items-center gap-1 text-blue-600 hover:text-blue-800 text-sm"
                        >
                          <ExternalLink className="w-4 h-4" />
                          Open File
                        </a>
                      )}
                    </div>
                    {result.exhibitor_count && (
                      <p className="text-sm text-gray-600 mt-1">
                        {result.exhibitor_count} exhibitors extracted
                      </p>
                    )}
                    {result.error && (
                      <p className="text-sm text-red-600 mt-1">{result.error}</p>
                    )}
                  </div>
                ))}
              </div>
            )}

            {jobStatus.error && (
              <p className="text-red-600 text-sm mt-2">{jobStatus.error}</p>
            )}
          </div>
        )}

        {/* Pending Requests Table */}
        <div className="bg-white shadow-sm rounded-lg border border-gray-200 overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
            <h2 className="text-lg font-medium text-gray-900">
              Pending Requests
              {pendingRequests.length > 0 && (
                <span className="ml-2 text-sm font-normal text-gray-500">
                  ({pendingRequests.length} items)
                </span>
              )}
            </h2>
            {selectedRows.size > 0 && (
              <button
                onClick={processSelected}
                disabled={polling}
                className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {polling ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Play className="w-4 h-4" />
                )}
                Process Selected ({selectedRows.size})
              </button>
            )}
          </div>

          {loading ? (
            <div className="p-8 text-center">
              <Loader2 className="w-8 h-8 animate-spin text-gray-400 mx-auto" />
              <p className="mt-2 text-gray-500">Loading pending requests...</p>
            </div>
          ) : pendingRequests.length === 0 ? (
            <div className="p-8 text-center text-gray-500">
              No pending requests found
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left">
                      <input
                        type="checkbox"
                        checked={selectedRows.size === pendingRequests.length}
                        onChange={selectAll}
                        className="w-4 h-4 rounded border-gray-300"
                      />
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Show Name
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Date
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Location
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Floorplan URL
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Exhibitor List
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {pendingRequests.map((request) => (
                    <tr
                      key={request.row_index}
                      className={`hover:bg-gray-50 ${
                        selectedRows.has(request.row_index) ? 'bg-blue-50' : ''
                      }`}
                    >
                      <td className="px-4 py-3">
                        <input
                          type="checkbox"
                          checked={selectedRows.has(request.row_index)}
                          onChange={() => toggleRow(request.row_index)}
                          className="w-4 h-4 rounded border-gray-300"
                        />
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <Building className="w-4 h-4 text-gray-400" />
                          <span className="font-medium text-gray-900">
                            {request.show_name || '-'}
                          </span>
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2 text-sm text-gray-600">
                          <Calendar className="w-4 h-4 text-gray-400" />
                          {formatDate(request.start_date)}
                          {request.end_date && ` - ${formatDate(request.end_date)}`}
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2 text-sm text-gray-600">
                          <MapPin className="w-4 h-4 text-gray-400" />
                          {request.location || '-'}
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        {request.floorplan_url ? (
                          <a
                            href={request.floorplan_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-blue-600 hover:text-blue-800 text-sm flex items-center gap-1"
                          >
                            <ExternalLink className="w-3 h-3" />
                            View
                          </a>
                        ) : (
                          <span className="text-gray-400 text-sm">-</span>
                        )}
                      </td>
                      <td className="px-4 py-3">
                        {request.exhibitor_list_url ? (
                          <a
                            href={request.exhibitor_list_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-blue-600 hover:text-blue-800 text-sm flex items-center gap-1"
                          >
                            <ExternalLink className="w-3 h-3" />
                            View
                          </a>
                        ) : (
                          <span className="text-gray-400 text-sm">-</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Info Card */}
        <div className="mt-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
          <h3 className="font-medium text-blue-900 mb-2">How It Works</h3>
          <ol className="text-sm text-blue-800 space-y-1 list-decimal list-inside">
            <li>Select one or more pending trade show requests from the table above</li>
            <li>Click "Process Selected" to start extraction</li>
            <li>The system will extract exhibitors from floorplans or exhibitor lists</li>
            <li>Contacts are enriched via Seamless.ai API</li>
            <li>XLSX files are generated and saved to Google Drive</li>
            <li>Completed files appear in the status panel with download links</li>
          </ol>
        </div>
      </main>
    </div>
  )
}

export default App
