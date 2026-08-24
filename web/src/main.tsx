import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import i18n from '@/lib/i18n'
import { $lang } from '@/stores/langStore'
import { $theme, THEMES } from '@/stores/themeStore'
import '@/index.css'
import App from '@/App'

// Composition-root wiring: stores → document/i18n.
$theme.subscribe((theme) => {
  const root = document.documentElement
  root.classList.remove(...THEMES)
  root.classList.add(theme)
})

$lang.subscribe((lang) => {
  void i18n.changeLanguage(lang)
  document.documentElement.lang = lang
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
