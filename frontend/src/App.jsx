import { useEffect, useState } from 'react';
import { io } from 'socket.io-client';
import axios from 'axios';

const socket = io('http://aed10aae28f6a47128a9856fe79a49e4-1227861173.ap-south-1.elb.amazonaws.com:4000');
const SEVERITY_COLORS = {
  CRITICAL: '#ff4444',
  HIGH:     '#ff8800',
  LOW:      '#00cc44',
};

export default function App() {
  const [drifts, setDrifts]     = useState([]);
  const [selected, setSelected] = useState(null);
  const [restoring, setRestoring] = useState(null);

  useEffect(() => {
    socket.on('drift:history', (history) => setDrifts(history));

    socket.on('drift:detected', (event) => {
      setDrifts(prev => [event, ...prev]);
    });

  socket.on('drift:report', ({ id, report, resourceName }) => {
  setDrifts(prev => prev.map(d => 
    d.resourceName === resourceName ? { ...d, report } : d
  ));
});

    socket.on('drift:restored', ({ resourceName }) => {
      setDrifts(prev => prev.map(d =>
        d.resourceName === resourceName ? { ...d, status: 'RESTORED' } : d
      ));
    });

    return () => socket.off();
  }, []);

  // Show only latest drift per resource
const uniqueDrifts = drifts.reduce((acc, drift) => {
  if (!acc.find(d => d.resourceName === drift.resourceName && d.status === drift.status)) {
    acc.push(drift);
  }
  return acc;
}, []);

  async function handleRestore(drift) {
    setRestoring(drift.id);
    try {
      await axios.post('http://aed10aae28f6a47128a9856fe79a49e4-1227861173.ap-south-1.elb.amazonaws.com:4000/api/restore', {
        resourceName: drift.resourceName,
        namespace:    drift.namespace || 'default',
        replicas:     3,
      });
    } catch (err) {
      alert('Restore failed: ' + err.message);
    }
    setRestoring(null);
  }

  return (
    <div style={{ background: '#0B0C10', minHeight: '100vh', color: '#fff', fontFamily: 'monospace', padding: '24px' }}>
      
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '32px' }}>
        <div style={{ width: '12px', height: '12px', borderRadius: '50%', background: '#00ff88', boxShadow: '0 0 8px #00ff88' }} />
        <h1 style={{ margin: 0, fontSize: '24px', color: '#FF7E40' }}>KubeVigil</h1>
        <span style={{ color: '#666', fontSize: '14px' }}>AI-powered drift detection</span>
        <div style={{ marginLeft: 'auto', color: '#666', fontSize: '12px' }}>
          {drifts.length} events tracked
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px' }}>
        
        {/* Left — Drift Feed */}
        <div>
          <h2 style={{ color: '#A64BFF', fontSize: '14px', marginBottom: '16px', textTransform: 'uppercase', letterSpacing: '2px' }}>
            Live Drift Feed
          </h2>
          {drifts.length === 0 && (
            <div style={{ color: '#444', padding: '24px', textAlign: 'center', border: '1px dashed #222', borderRadius: '8px' }}>
              Watching cluster... No drift detected.
            </div>
          )}
          {uniqueDrifts.map(drift => (
            <div
              key={drift.id}
              onClick={() => setSelected(drift)}
              style={{
                background:    selected?.id === drift.id ? '#1a1a2e' : '#111',
                border:        `1px solid ${selected?.id === drift.id ? '#A64BFF' : '#222'}`,
                borderLeft:    `4px solid ${SEVERITY_COLORS[drift.severity] || '#666'}`,
                borderRadius:  '8px',
                padding:       '16px',
                marginBottom:  '12px',
                cursor:        'pointer',
                transition:    'all 0.2s',
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                  <span style={{ color: '#FF7E40', fontWeight: 'bold' }}>{drift.resourceName}</span>
                  <span style={{ color: '#666', fontSize: '12px', marginLeft: '8px' }}>{drift.namespace}</span>
                </div>
                <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                  <span style={{
                    background: SEVERITY_COLORS[drift.severity] + '22',
                    color:      SEVERITY_COLORS[drift.severity],
                    padding:    '2px 8px',
                    borderRadius: '4px',
                    fontSize:   '11px',
                    fontWeight: 'bold',
                  }}>
                    {drift.severity}
                  </span>
                  <span style={{
                    background: drift.status === 'RESTORED' ? '#00cc4422' : '#ff444422',
                    color:      drift.status === 'RESTORED' ? '#00cc44'   : '#ff4444',
                    padding:    '2px 8px',
                    borderRadius: '4px',
                    fontSize:   '11px',
                  }}>
                    {drift.status}
                  </span>
                </div>
              </div>
              <div style={{ color: '#666', fontSize: '12px', marginTop: '8px' }}>
{new Date(drift.detectedAt).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}              </div>

              {drift.status === 'DRIFTED' && (
                <button
                  onClick={(e) => { e.stopPropagation(); handleRestore(drift); }}
                  disabled={restoring === drift.id}
                  style={{
                    marginTop:     '12px',
                    background:    restoring === drift.id ? '#333' : '#A64BFF',
                    color:         '#fff',
                    border:        'none',
                    borderRadius:  '6px',
                    padding:       '8px 16px',
                    cursor:        restoring === drift.id ? 'not-allowed' : 'pointer',
                    fontSize:      '12px',
                    fontWeight:    'bold',
                  }}
                >
                  {restoring === drift.id ? 'Restoring...' : '⟳ Restore to Git State'}
                </button>
              )}
            </div>
          ))}
        </div>

        {/* Right — Incident Report */}
        <div>
          <h2 style={{ color: '#A64BFF', fontSize: '14px', marginBottom: '16px', textTransform: 'uppercase', letterSpacing: '2px' }}>
            AI Incident Report
          </h2>
          {!selected && (
            <div style={{ color: '#444', padding: '24px', textAlign: 'center', border: '1px dashed #222', borderRadius: '8px' }}>
              Click a drift event to see the AI report
            </div>
          )}
          {selected && (
            <div style={{ background: '#111', border: '1px solid #222', borderRadius: '8px', padding: '20px' }}>
              <h3 style={{ color: '#FF7E40', marginTop: 0 }}>{selected.resourceName}</h3>

              {/* Diff */}
              <div style={{ marginBottom: '16px' }}>
                <div style={{ color: '#A64BFF', fontSize: '12px', marginBottom: '8px' }}>WHAT CHANGED</div>
                {selected.diffs?.filter(d => {
  const path = d.path?.join('.') || '';
  const noisyFields = ['terminationMessagePath', 'terminationMessagePolicy', 'imagePullPolicy', 'restartPolicy', 'terminationGracePeriodSeconds', 'dnsPolicy', 'securityContext', 'schedulerName', 'progressDeadlineSeconds', 'revisionHistoryLimit'];
  return !noisyFields.some(f => path.includes(f));
}).map((d, i) => (
  <div key={i} style={{ color: '#ccc', fontSize: '12px', background: '#0a0a0a', padding: '8px', borderRadius: '4px', marginBottom: '4px' }}>
    <span style={{ color: '#ff4444' }}>● </span>
    <span style={{ color: '#FF7E40' }}>{d.path?.join('.')}</span>
    <span style={{ color: '#666' }}> : </span>
    <span style={{ color: '#ff4444' }}>{JSON.stringify(d.lhs)}</span>
    <span style={{ color: '#666' }}> → </span>
    <span style={{ color: '#00cc44' }}>{JSON.stringify(d.rhs)}</span>
  </div>
))}
              </div>

              {/* AI Report */}
              {selected.report ? (
                <div>
                  <div style={{ color: '#A64BFF', fontSize: '12px', marginBottom: '8px' }}>AI ANALYSIS</div>
                  <pre style={{
                    color:      '#ccc',
                    fontSize:   '12px',
                    background: '#0a0a0a',
                    padding:    '12px',
                    borderRadius: '4px',
                    whiteSpace: 'pre-wrap',
                    wordBreak:  'break-word',
                    margin:     0,
                  }}>
                    {selected.report}
                  </pre>
                </div>
              ) : (
                <div style={{ color: '#666', fontSize: '12px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#A64BFF', animation: 'pulse 1s infinite' }} />
                  AI agent analyzing...
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}