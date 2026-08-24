import { useStore } from '@nanostores/react'
import { useTranslation } from 'react-i18next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { $lang, LANGS, setLang, type Lang } from '@/stores/langStore'
import { $theme, THEMES, setTheme, type Theme } from '@/stores/themeStore'

function App() {
  const { t } = useTranslation()
  const theme = useStore($theme)
  const lang = useStore($lang)

  return (
    <div className="flex min-h-svh items-center justify-center bg-background">
      <Card className="w-full max-w-md">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            {t('graphene.app.title')}
            <Badge variant="secondary">web</Badge>
          </CardTitle>
          <CardDescription>{t('graphene.app.stackDescription')}</CardDescription>
        </CardHeader>
        <CardContent>
          <Button className="w-full">{t('graphene.app.itWorks')}</Button>
        </CardContent>
        <CardFooter className="gap-2">
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm">
                {t('graphene.app.theme')}: {t(`graphene.theme.${theme}`)}
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start">
              <DropdownMenuRadioGroup
                value={theme}
                onValueChange={(value) => setTheme(value as Theme)}
              >
                {THEMES.map((id) => (
                  <DropdownMenuRadioItem key={id} value={id}>
                    {t(`graphene.theme.${id}`)}
                  </DropdownMenuRadioItem>
                ))}
              </DropdownMenuRadioGroup>
            </DropdownMenuContent>
          </DropdownMenu>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm">
                {t('graphene.app.language')}: {t(`graphene.lang.${lang}`)}
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start">
              <DropdownMenuRadioGroup
                value={lang}
                onValueChange={(value) => setLang(value as Lang)}
              >
                {LANGS.map((id) => (
                  <DropdownMenuRadioItem key={id} value={id}>
                    {t(`graphene.lang.${id}`)}
                  </DropdownMenuRadioItem>
                ))}
              </DropdownMenuRadioGroup>
            </DropdownMenuContent>
          </DropdownMenu>
        </CardFooter>
      </Card>
    </div>
  )
}

export default App
