document.addEventListener('DOMContentLoaded', () => {
  window.addEventListener('error', (e) => {
    console.error('Uncaught error:', e.message, e.error);
    alert('Startup error: ' + e.message);
  });
  console.log('script.js loaded');

  // -------------------------------
  // Global State
  // -------------------------------
  let allTransactions = [];
  let allCategories = []; // Dynamic, learned from DB (/api/categories)
  let allAccounts = [];
  let currentFilter = { type: null, value: null, name: null, period: 'all_time' };
  let spendingChart = null;
  let lastBudgetStatus = [];

  // -------------------------------
  // DOM Elements
  // -------------------------------
  const transactionList = document.getElementById('transaction-list');
  const transactionHeader = document.getElementById('transaction-header').querySelector('h2');
  const clearFilterBtn = document.getElementById('clear-filter-btn');
  const importForm = document.getElementById('import-form');
  const importAccountSelect = document.getElementById('import-account-select');
  const exportCsvBtn = document.getElementById('export-csv-btn');
  const importCorrectionsForm = document.getElementById('import-corrections-form');
  const aiCategorizeBtn = document.getElementById('ai-categorize-btn');
  const aiReportBtn = document.getElementById('ai-report-btn');
  const totalIncomeEl = document.getElementById('summary-income');
  const totalExpensesEl = document.getElementById('summary-expenses');
  const totalBalanceEl = document.getElementById('summary-balance');
  const totalSavingsEl = document.getElementById('summary-savings');
  const categorySummaryList = document.getElementById('category-summary-list');
  const addAccountForm = document.getElementById('add-account-form');
  const accountList = document.getElementById('account-list');
  const dateFilters = document.querySelector('.date-filters');
  const applyRulesBtn = document.getElementById('apply-rules-btn');
  const applyRulesForceBtn = document.getElementById('apply-rules-force-btn');
  const extractMerchantsBtn = document.getElementById('extract-merchants-btn');

  // Budget & Profile
  const profileForm = document.getElementById('profile-form');
  const annualIncomeInput = document.getElementById('annual-income');
  const annualIncomeDisplay = document.getElementById('annual-income-display');
  const monthlyIncomeDisplay = document.getElementById('monthly-income-display');
  const manageBudgetBtn = document.getElementById('manage-budget-btn');
  const budgetModal = document.getElementById('budget-modal');
  const closeBudgetModalBtn = document.getElementById('close-budget-modal-btn');
  const cancelBudgetBtn = document.getElementById('cancel-budget-btn');
  const saveBudgetBtn = document.getElementById('save-budget-btn');
  const proposeBudgetBtn = document.getElementById('propose-budget-btn');
  const budgetList = document.getElementById('budget-list');
  const aiReportModal = document.getElementById('ai-report-modal');
  const reportContentEl = document.getElementById('report-content');
  const closeModalBtn = document.getElementById('close-modal-btn');
  const budgetStatusModal = document.getElementById('budget-status-modal');
  const budgetStatusList = document.getElementById('budget-status-list');
  const closeStatusModalBtn = document.getElementById('close-status-modal-btn');
  const confirmStatusBtn = document.getElementById('confirm-status-btn');

  // -------------------------------
  // Event Listeners
  // -------------------------------
  addAccountForm.addEventListener('submit', handleAddAccount);
  importForm.addEventListener('submit', handleImportCSV);
  exportCsvBtn.addEventListener('click', () => { window.location.href = '/api/export'; });
  importCorrectionsForm.addEventListener('submit', handleImportCorrections);
  aiCategorizeBtn.addEventListener('click', handleAICategorize);
  aiReportBtn.addEventListener('click', handleAIReport);
  closeModalBtn.addEventListener('click', () => toggleModal(aiReportModal, false));
  aiReportModal.addEventListener('click', (e) => { if (e.target === aiReportModal) toggleModal(aiReportModal, false); });

  profileForm.addEventListener('submit', handleSaveProfile);
  manageBudgetBtn.addEventListener('click', handleManageBudget);
  closeBudgetModalBtn.addEventListener('click', () => toggleModal(budgetModal, false));
  cancelBudgetBtn.addEventListener('click', () => toggleModal(budgetModal, false));
  budgetModal.addEventListener('click', (e) => { if (e.target === budgetModal) toggleModal(budgetModal, false); });
  saveBudgetBtn.addEventListener('click', handleSaveBudget);
  proposeBudgetBtn.addEventListener('click', handleProposeBudget);
  closeStatusModalBtn.addEventListener('click', () => toggleModal(budgetStatusModal, false));
  confirmStatusBtn.addEventListener('click', () => toggleModal(budgetStatusModal, false));
  budgetStatusModal.addEventListener('click', (e) => { if (e.target === budgetStatusModal) toggleModal(budgetStatusModal, false); });

  if (dateFilters) {
    dateFilters.addEventListener('click', (e) => {
      const btn = e.target.closest('.date-filter-btn');
      if (!btn) return;
      currentFilter.period = btn.dataset.period;
      document.querySelectorAll('.date-filter-btn').forEach(b => b.classList.remove('active-filter'));
      btn.classList.add('active-filter');
      fetchAndRenderAllData();
    });
  }

  accountList.addEventListener('click', (e) => {
    const accountLink = e.target.closest('.account-link');
    if (!accountLink) return;
    e.preventDefault();
    if (accountLink.dataset.id === '') {
      currentFilter.type = null;
      currentFilter.value = null;
      currentFilter.name = null;
    } else {
      currentFilter.type = 'account';
      currentFilter.value = parseInt(accountLink.dataset.id, 10);
      currentFilter.name = accountLink.dataset.name;
    }
    renderTransactions();
  });

  categorySummaryList.addEventListener('click', (e) => {
    const categoryLink = e.target.closest('.category-link');
    if (!categoryLink) return;
    e.preventDefault();
    currentFilter = {
      ...currentFilter,
      type: 'category',
      value: categoryLink.dataset.category,
      name: categoryLink.dataset.category
    };
    renderTransactions();
  });

  clearFilterBtn.addEventListener('click', () => {
    currentFilter.type = null;
    currentFilter.value = null;
    currentFilter.name = null;
    renderTransactions();
  });

  // -------------------------------
  // Modal Logic
  // -------------------------------
  function toggleModal(modalElement, show) {
    const content = modalElement.querySelector('.modal-content');
    if (show) {
      modalElement.classList.remove('hidden');
      setTimeout(() => {
        modalElement.style.opacity = '1';
        content.style.transform = 'scale(1)';
      }, 10);
    } else {
      modalElement.style.opacity = '0';
      content.style.transform = 'scale(0.95)';
      setTimeout(() => modalElement.classList.add('hidden'), 300);
    }
  }

  // -------------------------------
  // API Helpers
  // -------------------------------
  async function loadCategories() {
    try {
      const res = await fetch('/api/categories');
      const text = await res.text();
      let data;
      try { data = JSON.parse(text); }
      catch { alert(`Error loading categories: ${text.slice(0,200)}`); return; }

      if (!res.ok) { alert(`Error loading categories: ${res.statusText}`); return; }
      const set = new Set((Array.isArray(data) ? data : []).filter(Boolean).map(String));
      set.add('Uncategorized');
      allCategories = Array.from(set).sort((a,b) => a.localeCompare(b));
    } catch {
      allCategories = ['Uncategorized'];
    }
  }

  // -------------------------------
  // Handlers
  // -------------------------------
  async function handleAddAccount(e) {
    e.preventDefault();
    const newAccountNameEl = document.getElementById('new-account-name');
    const name = newAccountNameEl.value.trim();
    if (!name) return;
    try {
      const response = await fetch('/api/accounts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      });
      if (!response.ok) throw new Error((await response.json()).error || 'Add account failed.');
      newAccountNameEl.value = '';
      fetchAndRenderAllData();
    } catch (error) {
      alert(`Error adding account: ${error.message}`);
    }
  }


  async function handleImportCSV(e) {
    e.preventDefault();
    const fileInput = document.getElementById('csv-file');
    const accountId = importAccountSelect.value;

    if (!fileInput.files[0] || !accountId) {
      return alert('Please select an account and a file.');
    }

    const formData = new FormData();
    formData.append('file', fileInput.files[0]);

    try {
      const response = await fetch(`/api/upload?account_id=${encodeURIComponent(accountId)}`, {
        method: 'POST',
        body: formData
      });

      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'Upload failed.');

      alert(result.message);
      fileInput.value = '';

      await fetchAndRenderAllData();
    } catch (error) {
      alert(`Error uploading file: ${error.message}`);
    }
  }

  async function handleImportCorrections(e) {
    e.preventDefault();
    const fileInput = document.getElementById('corrections-csv-file');
    if (!fileInput.files[0]) return alert('Please select a corrections file.');

    const formData = new FormData();
    formData.append('file', fileInput.files[0]);

    try {
      const resp = await fetch('/api/corrections/import', { method: 'POST', body: formData });
      const text = await resp.text();
      let data;
      try { data = JSON.parse(text); }
      catch { alert(`Error importing corrections: ${text.slice(0,200)}`); return; }

      if (!resp.ok || !data.ok) {
        alert(`Error importing corrections: ${data?.error || resp.statusText}`);
        return;
      }

      alert(`Import complete. Updated: ${data.updated}, Merged: ${data.merged}, Skipped: ${data.skipped}`);
      fileInput.value = '';
      await fetchAndRenderAllData();
    } catch (err) {
      alert(`Error importing corrections: ${err.message}`);
    }
  }

  async function handleAICategorize() {
    aiCategorizeBtn.disabled = true;
    aiCategorizeBtn.textContent = 'Thinking...';
    try {
      const response = await fetch('/api/ai-categorize', { method: 'POST' });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || result.message || 'AI categorize failed.');
      alert(result.message || 'AI categorization complete.');
      await fetchAndRenderAllData();
    } catch (error) {
      alert(`AI Categorization Error: ${error.message}`);
    } finally {
      aiCategorizeBtn.disabled = false;
      aiCategorizeBtn.textContent = 'Categorize with AI';
    }
  }

  async function handleAIReport() {
    reportContentEl.innerHTML = '<p class="text-center">Generating report...</p>';
    toggleModal(aiReportModal, true);
    try {
      const response = await fetch('/api/ai-financial-report', { method: 'POST' });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'AI report failed.');
      let htmlReport = (result.report || '').replace(/\n/g, '<br>');
      htmlReport = htmlReport.replace(/### (.*?)<br>/g, '<h3>$1</h3>');
      htmlReport = htmlReport.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
      htmlReport = htmlReport.replace(/\* (.*?)<br>/g, '<li>$1</li>');
      reportContentEl.innerHTML = htmlReport || '<p>No content generated.</p>';
    } catch (error) {
      reportContentEl.innerHTML = `<p class="text-red-500"><strong>Error:</strong> ${error.message}</p>`;
    }
  }

  async function handleSaveProfile(e) {
    e.preventDefault();
    const income = parseFloat(annualIncomeInput.value);
    if (isNaN(income) || income < 0) {
      return alert('Please enter a valid annual income.');
    }
    try {
      const response = await fetch('/api/profile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ annual_after_tax_income: income }),
      });
      if (!response.ok) throw new Error('Failed to save profile.');
      renderProfile({ annual_after_tax_income: income });
      alert('Profile saved!');
    } catch (error) {
      alert(`Error: ${error.message}`);
    }
  }

  async function handleManageBudget() {
    const [budgets, catSummary, historicalAvg] = await Promise.all([
      fetch('/api/budgets').then(res => res.json()),
      fetch('/api/category-summary').then(res => res.json()),
      fetch('/api/historical-spending').then(res => res.json())
    ]);

    const spentCategories = catSummary.map(c => c.category);
    const budgetedCategories = budgets.map(b => b.category);
    const allRelevantCategories = [...new Set([...spentCategories, ...budgetedCategories, 'Savings'])].sort();

    renderBudgetModal(budgets, allRelevantCategories, historicalAvg);
    toggleModal(budgetModal, true);

    document.getElementById('save-budget-btn').addEventListener('click', handleSaveBudget);
  }

  function renderBudgetStatusModal(budgetStatus) {
    budgetStatusList.innerHTML = '';
    if (!budgetStatus || budgetStatus.length === 0) {
      budgetStatusList.innerHTML = '<p class="p-4 text-center text-gray-400">No budget limits have been set.</p>';
      return;
    }

    budgetStatus.sort((a, b) => (a.limit_amount - a.spent) - (b.limit_amount - b.spent));
    budgetStatus.forEach(item => {
      const isOverBudget = item.spent > item.limit_amount;
      const remaining = item.limit_amount - item.spent;
      const row = document.createElement('div');
      row.className = 'budget-status-row';
      if (isOverBudget) row.classList.add('bg-red-900/50');

      row.innerHTML = `
        <div class="category">${item.category}</div>
        <div class="amount text-yellow-400">$${formatNumber(item.spent)}</div>
        <div class="amount text-gray-400">$${formatNumber(item.limit_amount)}</div>
        <div class="amount font-semibold ${remaining < 0 ? 'text-red-500' : 'text-green-500'}">
          $${formatNumber(remaining)}
        </div>
      `;
      budgetStatusList.appendChild(row);
    });
  }

  async function handleProposeBudget() {
    proposeBudgetBtn.disabled = true;
    proposeBudgetBtn.textContent = 'Analyzing...';
    try {
      const response = await fetch('/api/propose-budget', { method: 'POST' });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'AI proposal failed.');

      const proposed = result.proposed_budget;
      for (const category in proposed) {
        const input = budgetList.querySelector(`input[data-category="${category}"]`);
        if (input) input.value = Math.round(proposed[category]);
      }
      alert('AI budget proposal has been populated.');
    } catch (error) {
      alert(`Error: ${error.message}`);
    } finally {
      proposeBudgetBtn.disabled = false;
      proposeBudgetBtn.textContent = 'Propose AI Budget';
    }
  }

  async function handleSaveBudget() {
    const budgetInputs = budgetList.querySelectorAll('input');
    const newBudget = {};
    budgetInputs.forEach(input => {
      const value = parseFloat(input.value);
      if (!isNaN(value) && value >= 0) newBudget[input.dataset.category] = value;
    });

    try {
      const response = await fetch('/api/budgets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newBudget),
      });
      if (!response.ok) {
        const errData = await response.json();
        throw new Error(errData.error || `Failed to save budget. Server responded with ${response.status}`);
      }

      toggleModal(budgetModal, false);

      const statusResponse = await fetch('/api/budget-status');
      if (!statusResponse.ok) {
        const errData = await statusResponse.json();
        throw new Error(errData.error || `Failed to fetch budget status. Server responded with ${statusResponse.status}`);
      }
      const newBudgetStatus = await statusResponse.json();

      renderBudgetStatusModal(newBudgetStatus);
      toggleModal(budgetStatusModal, true);

      fetchAndRenderAllData();
    } catch (error) {
      alert(`Error: ${error.message}`);
    }
  }

  async function postJSON(url) {
    const resp = await fetch(url, { method: 'POST' });
    const text = await resp.text();
    let data;
    try { data = JSON.parse(text); }
    catch { throw new Error(text.slice(0,200)); }
    if (!resp.ok || data.ok === false) throw new Error(data.error || resp.statusText);
    return data;
  }

  if (applyRulesBtn) {
    applyRulesBtn.addEventListener('click', async () => {
      try {
        applyRulesBtn.disabled = true;
        applyRulesBtn.textContent = 'Applying rules...';
        const out = await postJSON('/api/apply-rules');
        alert(out.message || 'Rules applied.');
        fetchAndRenderAllData();
      } catch (e) {
        alert(`Apply rules failed: ${e.message}`);
      } finally {
        applyRulesBtn.disabled = false;
        applyRulesBtn.textContent = 'Rules → Apply Learned (safe)';
      }
    });
  }

  if (applyRulesForceBtn) {
    applyRulesForceBtn.addEventListener('click', async () => {
      if (!confirm('This will overwrite existing category/subcategory/merchant where a rule matches. Continue?')) return;
      try {
        applyRulesForceBtn.disabled = true;
        applyRulesForceBtn.textContent = 'Overwriting...';
        const out = await postJSON('/api/apply-rules?force=1');
        alert(out.message || 'Rules applied with overwrite.');
        fetchAndRenderAllData();
      } catch (e) {
        alert(`Force apply failed: ${e.message}`);
      } finally {
        applyRulesForceBtn.disabled = false;
        applyRulesForceBtn.textContent = 'Rules → Apply Learned (OVERWRITE)';
      }
    });
  }

  if (extractMerchantsBtn) {
    extractMerchantsBtn.addEventListener('click', async () => {
      try {
        extractMerchantsBtn.disabled = true;
        extractMerchantsBtn.textContent = 'Extracting merchants...';
        const out = await postJSON('/api/extract-merchants');
        alert(out.message || 'Merchant extraction complete.');
        fetchAndRenderAllData();
      } catch (e) {
        alert(`Extract merchants failed: ${e.message}`);
      } finally {
        extractMerchantsBtn.disabled = false;
        extractMerchantsBtn.textContent = 'Merchants → Extract names for blanks';
      }
    });
  }

  // -------------------------------
  // Fetch & Render
  // -------------------------------
  async function fetchAndRenderAllData() {
    try {
      const { startDate, endDate } = getDateRange(currentFilter.period);
      const params = new URLSearchParams();
      if (startDate) params.append('start_date', startDate);
      if (endDate) params.append('end_date', endDate);

      await loadCategories();

      const [accRes, txRes, sumRes, catSumRes, profRes, budgRes] = await Promise.all([
        fetch('/api/accounts'),
        fetch(`/api/transactions?${params.toString()}`),
        fetch(`/api/summary?${params.toString()}`),
        fetch(`/api/category-summary?${params.toString()}`),
        fetch('/api/profile'),
        fetch(`/api/budget-status?${params.toString()}`)
      ]);

      allAccounts = await accRes.json();
      allTransactions = await txRes.json();
      const summary = await sumRes.json();
      const catSummary = await catSumRes.json();
      const profile = await profRes.json();
      lastBudgetStatus = await budgRes.json();

      currentFilter.type = null;
      currentFilter.value = null;
      currentFilter.name = null;

      // Use ONLY true Income for the Total Income card
      const uiIncome = getIncomeFromCategorySummary(catSummary);
      renderAll({ ...summary, income: uiIncome }, catSummary, profile, lastBudgetStatus);
    } catch (error) {
      console.error('Error fetching data:', error);
    }
  }

  async function refreshSummariesOnly() {
    try {
      const { startDate, endDate } = getDateRange(currentFilter.period);
      const params = new URLSearchParams();
      if (startDate) params.append('start_date', startDate);
      if (endDate) params.append('end_date', endDate);

      const [sumRes, catSumRes, budgRes] = await Promise.all([
        fetch(`/api/summary?${params.toString()}`),
        fetch(`/api/category-summary?${params.toString()}`),
        fetch(`/api/budget-status?${params.toString()}`)
      ]);

      const summary = await sumRes.json();
      const catSummary = await catSumRes.json();
      const budgetStatus = await budgRes.json();

      const uiIncome = getIncomeFromCategorySummary(catSummary);
      updateSummary({ ...summary, income: uiIncome });
      updateCategorySummaryAndTracking(catSummary, budgetStatus);
      renderSpendingChart(catSummary);
    } catch (e) {
      console.error('Failed to refresh summaries:', e);
    }
  }

  function renderAll(summary, catSummary, profile, budgetStatus) {
    renderAccounts();
    renderTransactions();
    renderProfile(profile);
    updateSummary(summary);
    updateCategorySummaryAndTracking(catSummary, budgetStatus);
    renderSpendingChart(catSummary);
  }

  // -------------------------------
  // Rendering
  // -------------------------------
  function renderAccounts() {
    accountList.innerHTML = '';
    importAccountSelect.innerHTML = `
      <option value="">Select an account...</option>
      <option value="csv">-- Read Account from CSV File --</option>
    `;

    const allAccountsLink = document.createElement('a');
    allAccountsLink.href = '#';
    allAccountsLink.className = 'account-link block text-sm p-1 rounded font-bold hover:bg-gray-100';
    allAccountsLink.dataset.id = '';
    allAccountsLink.textContent = 'All Accounts';
    accountList.appendChild(allAccountsLink);

    allAccounts.forEach(acc => {
      const accountEl = document.createElement('a');
      accountEl.href = '#';
      accountEl.className = 'account-link block text-sm p-1 rounded hover:bg-gray-100';
      accountEl.dataset.id = acc.id;
      accountEl.dataset.name = acc.name;
      accountEl.textContent = acc.name;
      accountList.appendChild(accountEl);

      const optionEl = document.createElement('option');
      optionEl.value = acc.id;
      optionEl.textContent = acc.name;
      importAccountSelect.appendChild(optionEl);
    });
  }

  function renderTransactions() {
    transactionList.innerHTML = '';
    const hiddenCategories = ['Card Payment', 'Savings', 'Financial Transactions'];
    let list = allTransactions.filter(t => !hiddenCategories.includes(t.category));
    clearFilterBtn.classList.add('hidden');

    if (currentFilter.type === 'account' && currentFilter.value) {
      list = list.filter(t => t.account_id === currentFilter.value);
      transactionHeader.textContent = `Transactions: ${currentFilter.name}`;
      clearFilterBtn.classList.remove('hidden');
    } else if (currentFilter.type === 'category' && currentFilter.value) {
      list = allTransactions.filter(t => t.category === currentFilter.value);
      transactionHeader.textContent = `Transactions: ${currentFilter.name}`;
      clearFilterBtn.classList.remove('hidden');
    } else {
      transactionHeader.textContent = 'All Transactions';
    }

    if (list.length === 0) {
      transactionList.innerHTML = `<p class="text-gray-500">No transactions to display.</p>`;
      return;
    }

    list.forEach(t => {
      const isIncome = Number(t.amount) > 0;
      const amountColor = isIncome ? 'text-green-600' : 'text-red-600';
      const sign = isIncome ? '+' : '-';

      const currentCategory = (t.category && t.category.trim()) ||
                              (t.ai_category && t.ai_category.trim()) ||
                              'Uncategorized';

      const categorySet = new Set(allCategories);
      categorySet.add('Uncategorized');
      if (currentCategory) categorySet.add(currentCategory);
      const categoriesForRow = Array.from(categorySet).sort((a, b) => a.localeCompare(b));

      const categoryOptions = [
        `<option value="" disabled>— choose —</option>`,
        ...categoriesForRow.map(cat =>
          `<option value="${escapeHtml(cat)}"${cat === currentCategory ? ' selected' : ''}>${escapeHtml(cat)}</option>`
        )
      ].join('');

      const el = document.createElement('div');
      el.className = 'p-2 border rounded flex justify-between items-center';
      el.innerHTML = `
        <div>
          <div class="text-sm font-medium">${escapeHtml(t.merchant || t.cleaned_description || '')}</div>
          <div class="text-xs text-gray-500">${escapeHtml(fmtMMDDYY(t.transaction_date))} • ${escapeHtml(t.account_name)}</div>
        </div>
        <div class="flex items-center space-x-3">
          <span class="${amountColor} font-semibold">
            ${sign}$${formatNumber(Math.abs(Number(t.amount)))}
          </span>
          <select
            class="category-select input-field text-sm"
            data-txid="${String(t.transaction_id)}"
            data-description="${escapeHtml((t.merchant || t.cleaned_description || '').replace(/"/g,'&quot;'))}">
            ${categoryOptions}
          </select>
        </div>
      `;
      transactionList.appendChild(el);
    });
  }

  function renderProfile(profile) {
    if (profile && profile.annual_after_tax_income) {
      const annualIncome = profile.annual_after_tax_income;
      const monthlyIncome = annualIncome / 12;
      annualIncomeInput.value = annualIncome;
      annualIncomeDisplay.textContent = `$${formatNumber(annualIncome)}`;
      monthlyIncomeDisplay.textContent = `Monthly Income: $${formatNumber(monthlyIncome)}`;
    } else {
      annualIncomeDisplay.textContent = '';
      monthlyIncomeDisplay.textContent = 'Set income to enable AI proposals.';
    }
  }

  function renderBudgetModal(budgets, categories, historicalAvg) {
    const budgetMap = new Map(budgets.map(b => [b.category, b.limit_amount]));
    const historicalAvgMap = new Map(Object.entries(historicalAvg));

    budgetList.innerHTML = categories.map(cat => {
      const limit = budgetMap.get(cat) || 0;
      const averages = historicalAvgMap.get(cat) || {};
      const avg1m = averages.avg_1m || 0;
      const avg3m = averages.avg_3m || 0;
      const avg6m = averages.avg_6m || 0;
      const avg18m = averages.avg_18m || 0;

      return `
        <div class="budget-settings-row">
          <label class="category-label">${escapeHtml(cat)}</label>
          <div class="avg-amount">$${formatNumber(avg1m)}</div>
          <div class="avg-amount">$${formatNumber(avg3m)}</div>
          <div class="avg-amount">$${formatNumber(avg6m)}</div>
          <div class="avg-amount">$${formatNumber(avg18m)}</div>
          <div class="flex items-center space-x-2 justify-end">
            <span class="text-gray-500">$</span>
            <input type="number" class="input-field w-24 text-right" value="${limit}" data-category="${escapeHtml(cat)}" placeholder="0">
          </div>
        </div>
      `;
    }).join('');
  }

  // === NEW: category summary rendering with aggregation ===
  function updateCategorySummaryAndTracking(catSummary, budgetStatus) {
    categorySummaryList.innerHTML = '';

    const agg = aggregateByCategory(Array.isArray(catSummary) ? catSummary : []);
    const budgetMap = new Map((Array.isArray(budgetStatus) ? budgetStatus : []).map(b => [b.category, b]));

    // Expenses (negative)
    const expenseSummary = agg
      .filter(c => parseNum(c.total) < 0)
      .sort((a, b) => Math.abs(parseNum(b.total)) - Math.abs(parseNum(a.total)));

    expenseSummary.forEach(({ category, total }) => {
      const budgetInfo = budgetMap.get(category);
      const limit = budgetInfo ? budgetInfo.limit_amount : 0;
      const spent = Math.abs(parseNum(total));
      const progress = limit > 0 ? (spent / limit) * 100 : 0;
      const progressBarColor = progress > 100 ? 'bg-red-500'
                          : progress > 85  ? 'bg-yellow-500'
                          : 'bg-green-500';

      const row = document.createElement('div');
      row.className = 'category-summary-item';
      row.innerHTML = `
        <a href="#" class="category-link block text-sm hover:font-bold flex justify-between items-center" data-category="${escapeHtml(category)}">
          <span>${escapeHtml(category)}</span>
          <strong class="text-red-600">-$${formatNumber(spent)}</strong>
        </a>
        ${limit > 0 ? `
        <div class="mt-1">
          <div class="w-full bg-gray-200 rounded-full h-2">
            <div class="${progressBarColor}" style="width:${Math.min(progress, 100)}%; height:100%; border-radius:9999px;"></div>
          </div>
          <div class="text-xs text-gray-500 text-right mt-0.5">$${formatNumber(spent)} of $${formatNumber(limit)}</div>
        </div>` : ''}
      `;
      categorySummaryList.appendChild(row);
    });

    // Inflows (positive totals EXCEPT 'Income')
    const inflows = agg
      .filter(c => parseNum(c.total) > 0 && String(c.category) !== 'Income')
      .sort((a, b) => parseNum(b.total) - parseNum(a.total));

    if (inflows.length) {
      const label = document.createElement('div');
      label.className = 'text-xs uppercase text-gray-400 mt-4 mb-1 tracking-wide';
      label.textContent = 'Inflows';
      categorySummaryList.appendChild(label);

      inflows.forEach(({ category, total }) => {
        const row = document.createElement('div');
        row.className = 'category-summary-item';
        row.innerHTML = `
          <a href="#" class="category-link block text-sm hover:font-bold flex justify-between items-center" data-category="${escapeHtml(category)}">
            <span>${escapeHtml(category)}</span>
            <strong class="text-green-600">$${formatNumber(parseNum(total))}</strong>
          </a>
        `;
        categorySummaryList.appendChild(row);
      });
    }

    // Income (single line if present)
    const income = agg.find(c => String(c.category) === 'Income' && parseNum(c.total) > 0);
    if (income) {
      const sep = document.createElement('div');
      sep.className = 'mt-4 mb-2 text-xs uppercase tracking-wide text-gray-400';
      sep.textContent = 'Income';
      categorySummaryList.appendChild(sep);

      const row = document.createElement('div');
      row.className = 'category-summary-item';
      row.innerHTML = `
        <a href="#" class="category-link block text-sm hover:font-bold flex justify-between items-center" data-category="Income">
          <span>Income</span>
          <strong class="text-green-600">$${formatNumber(Math.abs(parseNum(income.total)))}</strong>
        </a>
      `;
      categorySummaryList.appendChild(row);
    }
  }

  function renderSpendingChart(catSummaryData = []) {
    const canvas = document.getElementById('spending-chart');
    if (!canvas) return;

    let catAgg = aggregateByCategory(catSummaryData);
    if ((!Array.isArray(catAgg) || catAgg.length === 0) && Array.isArray(catSummaryData) && catSummaryData.length > 0) {
      catAgg = catSummaryData.map(r => ({ category: r.category, total: parseNum(r.total) }));
    }

    const expenseRows = catAgg.filter(x => parseNum(x.total) < 0);
    const labels = expenseRows.map(r => r.category);
    const data   = expenseRows.map(r => Math.abs(parseNum(r.total)));

    const ctx = canvas.getContext('2d');
    if (spendingChart) spendingChart.destroy();
    spendingChart = new Chart(ctx, {
      type: 'doughnut',
      data: { labels, datasets: [{ data }] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom' },
          tooltip: {
            callbacks: { label: (item) => `${item.label}: $${formatNumber(item.raw)}` }
          }
        }
      }
    });
  }

  function updateSummary(s) {
    const prettyAbs = (n) => formatNumber(Math.abs(Number(n) || 0));
    const exp = Number(s.expenses) || 0;
    const bal = Number(s.balance) || 0;

    totalIncomeEl.textContent = `$${prettyAbs(s.income)}`; // Already overridden to "Income" only
    totalExpensesEl.textContent = `${exp < 0 ? '-$' : '$'}${prettyAbs(exp)}`;
    totalBalanceEl.textContent = `${bal < 0 ? '-$' : '$'}${prettyAbs(bal)}`;
    totalSavingsEl.textContent = `$${prettyAbs(s.savings || 0)}`;

    const balanceCard = totalBalanceEl.closest('.summary-card');
    balanceCard.classList.remove('positive', 'negative');
    balanceCard.classList.add(bal >= 0 ? 'positive' : 'negative');
  }

  // -------------------------------
  // Helpers
  // -------------------------------
  function parseNum(x) {
    if (typeof x === 'number') return isFinite(x) ? x : 0;
    if (x == null) return 0;
    const n = parseFloat(String(x).replace(/,/g, '').trim());
    return isFinite(n) ? n : 0;
  }

  function aggregateByCategory(rows = []) {
    const map = new Map();
    for (const r of (Array.isArray(rows) ? rows : [])) {
      const rawCat = r.category == null ? 'Uncategorized' : String(r.category);
      const name = rawCat.replace(/\u00A0/g, ' ').trim() || 'Uncategorized';
      const val = parseNum(r.total);
      map.set(name, (map.get(name) || 0) + val);
    }
    return Array.from(map, ([category, total]) => ({ category, total }));
  }

  function getIncomeFromCategorySummary(catSummary) {
    const agg = aggregateByCategory(Array.isArray(catSummary) ? catSummary : []);
    const row = agg.find(r => String(r.category) === 'Income');
    return row ? Math.max(0, parseNum(row.total)) : 0;
  }

  function formatNumber(value) {
    if (value === null || isNaN(value)) return '0';
    return Math.round(Number(value)).toLocaleString('en-US');
  }

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  function fmtMMDDYY(iso) {
    if (!iso || typeof iso !== 'string') return iso || '';
    const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return iso;
    const [, y, mo, d] = m;
    return `${mo}-${d}-${String(y).slice(2)}`;
  }

  function getDateRange(period) {
    const today = new Date();
    const y = today.getFullYear();
    const fmt = (d) => d.toISOString().slice(0, 10);

    if (period === 'this_month') {
      const startDate = new Date(y, today.getMonth(), 1);
      return { startDate: fmt(startDate), endDate: fmt(today) };
    }
    if (period === 'last_month') {
      const endDate = new Date(y, today.getMonth(), 0);
      const startDate = new Date(endDate.getFullYear(), endDate.getMonth(), 1);
      return { startDate: fmt(startDate), endDate: fmt(endDate) };
    }
    if (period === 'last_3_months') {
      const startDate = new Date(); startDate.setMonth(startDate.getMonth() - 3);
      return { startDate: fmt(startDate), endDate: fmt(today) };
    }
    if (period === 'last_6_months') {
      const startDate = new Date(); startDate.setMonth(startDate.getMonth() - 6);
      return { startDate: fmt(startDate), endDate: fmt(today) };
    }
    if (period === 'this_year') {
      const startDate = new Date(y, 0, 1);
      return { startDate: fmt(startDate), endDate: fmt(today) };
    }
    if (period === 'all_time') {
      return { startDate: '', endDate: '' };
    }
    if (period === 'last_year') {
      const lastYear = y - 1;
      const startDate = new Date(lastYear, 0, 1);
      const endDate = new Date(lastYear, 11, 31);
      return { startDate: fmt(startDate), endDate: fmt(endDate) };
    }
    return { startDate: '', endDate: '' };
  }

  // -------------------------------
  // Kick off
  // -------------------------------
  fetchAndRenderAllData();
});


/* PLAID-LINK-START */
async function plaidCreateToken() {
  try {
    const r = await fetch('/plaid/create_link_token', { method:'POST' });
    if (!r.ok) {
      const text = await r.text();
      alert('Plaid error (create): HTTP ' + r.status + ' ' + text.slice(0,200));
      return null;
    }
    const d = await r.json();
    if (!d.link_token) {
      alert('Plaid error: no link_token in response');
      return null;
    }
    return d.link_token;
  } catch (e) {
    alert('Plaid error (create): ' + e);
    return null;
  }
}

async function openPlaidLink() {
  const token = await plaidCreateToken();
  if (!token) return;

  const handler = Plaid.create({
    token,
    onSuccess: async function(public_token, metadata) {
      try {
        const r = await fetch('/plaid/exchange_public_token', {
          method: 'POST',
          headers: {'Content-Type':'application/json'},
          body: JSON.stringify({ public_token })
        });
        const d = await r.json().catch(()=> ({}));
        if (r.ok) {
          const out = document.getElementById('plaid-result');
          if (out) out.textContent = 'Linked ✓ item_id=' + (d.item_id || '?');
        } else {
          alert('Plaid exchange failed: ' + (d.error_message || JSON.stringify(d)).slice(0,300));
        }
      } catch (e) {
        alert('Plaid exchange error: ' + e);
      }
    },
    onExit: function(err, metadata) {
      if (err) console.warn('Plaid exit:', err);
    }
  });
  handler.open();
}

document.getElementById('plaid-link-btn')?.addEventListener('click', openPlaidLink);
/* PLAID-LINK-END */
