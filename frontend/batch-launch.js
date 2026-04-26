/**
 * Batch Launch: multi-asset multi-account support
 * Hooks into existing functions via monkey-patching
 */

// ── State ──
var _selectedAssetIds = new Set();

// ── Toggle selection ──
function toggleAssetSelection(aid) {
  if (_selectedAssetIds.has(aid)) _selectedAssetIds.delete(aid);
  else _selectedAssetIds.add(aid);
  _updateBatchBtn();
  renderAssetsList(_assetsData || []);
}

function _updateBatchBtn() {
  var btn = document.getElementById('batchLaunchBar');
  var ids = _selectedAssetIds ? Array.from(_selectedAssetIds) : [];
  if (btn) {
    btn.style.display = ids.length ? '' : 'none';
    var ct = btn.querySelector('.batchCount');
    if (ct) ct.textContent = ids.length;
  }
}

// ── Monkey-patch _renderAssetCard to add checkbox ──
var _origRenderAssetCard = _renderAssetCard;
_renderAssetCard = function(a) {
  var _isSel = _selectedAssetIds && _selectedAssetIds.has(a.id);
  var card = _origRenderAssetCard(a);
  // Checkbox HTML (no double quotes to avoid breaking onerror attributes)
  var checkbox = '<div style="position:absolute;top:6px;left:6px;z-index:5">' +
    '<input type="checkbox" ' + (_isSel ? 'checked' : '') + ' ' +
    'onclick="event.stopPropagation();toggleAssetSelection(' + a.id + ')" ' +
    'style="width:16px;height:16px;cursor:pointer;accent-color:var(--ac)">' +
    '</div>';

  // Find the thumbnail preview div closing correctly.
  // The preview div is followed by the info section:
  //   <div style="display:flex;align-items:flex-start;gap:8px;margin-bottom:8px">
  // We search for the </div> that closes the preview div (the one right before info section).
  var infoMarker = 'display:flex;align-items:flex-start;gap:8px;margin-bottom:8px';
  var infoStart = card.indexOf(infoMarker);
  if (infoStart > 0) {
    // Find the </div> that closes the preview div (last one before info section)
    var thumbEnd = card.lastIndexOf('</div>', infoStart);
    if (thumbEnd > 0) {
      card = card.slice(0, thumbEnd) + checkbox + card.slice(thumbEnd);
    }
  } else {
    // Fallback: use the original (less reliable) method
    var thumbEnd = card.indexOf('</div>', card.indexOf('height:140px'));
    if (thumbEnd > 0) {
      card = card.slice(0, thumbEnd) + checkbox + card.slice(thumbEnd);
    }
  }

  // Add selection border
  var styleIdx = card.indexOf('style="cursor:default"');
  if (_isSel && styleIdx > 0) {
    card = card.slice(0, styleIdx + 6) + 'outline:2px solid var(--ac);outline-offset:-2px;border-radius:var(--rs);' + card.slice(styleIdx + 6);
  }
  return card;
};

// ── Batch launch button + modal ──
function openBatchLaunchModal() {
  var ids = Array.from(_selectedAssetIds || []);
  if (!ids.length) return alert('请先选择要发布的素材');
  window._batchAssetIds = ids;
  openLaunchModal(ids[0]);
  setTimeout(function() {
    var title = document.querySelector('#moLaunchCampaign .modal-title');
    if (title) title.textContent = '批量发布 (' + ids.length + ' 个素材)';
  }, 100);
}

// ── Monkey-patch the batch-launch API call ──
var _origApi = window.api;
window.api = async function(method, url, data) {
  if (method === 'POST' && url.indexOf('/batch-launch') > 0 && window._batchAssetIds && window._batchAssetIds.length > 0) {
    var ids = window._batchAssetIds;
    if (ids.length === 1) {
      window._batchAssetIds = null;
      return _origApi(method, '/assets/' + ids[0] + '/batch-launch', data);
    }
    data.asset_ids = ids;
    window._batchAssetIds = null;
    return _origApi(method, '/assets/batch-launch', data);
  }
  return _origApi(method, url, data);
};

// ── Batch launch bar UI ──
document.addEventListener('DOMContentLoaded', function() {
  var toolbar = document.querySelector('#assetsViewMode');
  if (toolbar) {
    var parent = toolbar.closest('div[style*="display:flex"]') || toolbar.parentElement;
    if (parent && !document.getElementById('batchLaunchBar')) {
      var bar = document.createElement('div');
      bar.id = 'batchLaunchBar';
      bar.style.cssText = 'display:none;padding:8px 12px;background:var(--bg2);border:1px solid var(--ac);border-radius:12px;margin-bottom:10px';
      bar.innerHTML = '<span style="font-size:13px;font-weight:600">已选择 <span class="batchCount" style="color:var(--ac)">0</span> 个素材</span>' +
        '<button class="btn btn-p btn-sm" onclick="openBatchLaunchModal()" style="margin-left:12px">🚀 批量发布</button>' +
        '<button class="btn btn-sm" onclick="clearAssetSelection()" style="margin-left:6px">取消选择</button>';
      var dropzone = document.getElementById('assetDropZone');
      if (dropzone) dropzone.parentElement.insertBefore(bar, dropzone.nextSibling);
    }
  }
});

function clearAssetSelection() {
  _selectedAssetIds.clear();
  _updateBatchBtn();
  renderAssetsList(_assetsData || []);
}
