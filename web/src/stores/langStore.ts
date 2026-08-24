import { persistentAtom } from '@nanostores/persistent'

export const LANGS = ['en', 'ru'] as const
export type Lang = (typeof LANGS)[number]

export const DEFAULT_LANG: Lang = 'en'

function isLang(value: string): value is Lang {
  return (LANGS as readonly string[]).includes(value)
}

export const $lang = persistentAtom<Lang>('graphene.lang', DEFAULT_LANG, {
  encode: (lang) => lang,
  decode: (raw) => (isLang(raw) ? raw : DEFAULT_LANG),
})

export function setLang(lang: Lang) {
  $lang.set(lang)
}
