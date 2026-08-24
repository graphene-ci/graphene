import { persistentAtom } from '@nanostores/persistent'

export const THEMES = [
  'light',
  'snow',
  'paper',
  'dark',
  'midnight',
  'graphite',
] as const
export type Theme = (typeof THEMES)[number]

export const DEFAULT_THEME: Theme = 'dark'

function isTheme(value: string): value is Theme {
  return (THEMES as readonly string[]).includes(value)
}

export const $theme = persistentAtom<Theme>('graphene.theme', DEFAULT_THEME, {
  encode: (theme) => theme,
  decode: (raw) => (isTheme(raw) ? raw : DEFAULT_THEME),
})

export function setTheme(theme: Theme) {
  $theme.set(theme)
}
