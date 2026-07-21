// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Tencent. All rights reserved.

import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import * as Dialog from '@radix-ui/react-dialog';
import { X, Maximize2, Minimize2, RotateCcw, Plus, Minus, Loader2 } from 'lucide-react';
import '@xterm/xterm/css/xterm.css';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { useTerminal } from './useTerminal';

interface Props {
  sandboxID: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function TerminalDialog({ sandboxID, open, onOpenChange }: Props) {
  const { t } = useTranslation('terminal');
  const [fullscreen, setFullscreen] = useState(false);
  const {
    containerRef,
    status,
    exitCode,
    errorMessage,
    reconnect,
    fontSize,
    increaseFontSize,
    decreaseFontSize,
  } = useTerminal(sandboxID, open);

  const handleOpenChange = (next: boolean) => {
    if (!next) setFullscreen(false);
    onOpenChange(next);
  };

  const statusText: Record<string, string> = {
    connecting: t('status.connecting'),
    disconnected: t('status.disconnected'),
    exited: exitCode != null ? t('exitCode', { code: exitCode }) : t('status.exited'),
    error: errorMessage ?? t('status.error'),
  };

  return (
    <Dialog.Root open={open} onOpenChange={handleOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-40 bg-background/70 backdrop-blur-sm data-[state=open]:animate-fade-in" />
        <Dialog.Content
          className={cn(
            'fixed z-50 flex flex-col overflow-hidden border border-border/60 bg-card shadow-2xl',
            fullscreen
              ? 'inset-2 rounded-xl'
              : 'left-1/2 top-1/2 h-[min(640px,calc(100vh-3rem))] w-[min(920px,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 rounded-2xl',
          )}
        >
          {/* Header */}
          <div className="flex items-center gap-3 border-b border-border/60 px-4 py-3">
            <Dialog.Title className="text-sm font-semibold">
              {t('title')}
              <span className="ml-2 font-mono text-xs font-normal text-muted-foreground">
                {sandboxID}
              </span>
            </Dialog.Title>
            <span
              className={cn(
                'inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[11px] ring-1',
                status === 'ready'
                  ? 'bg-cube-ok/10 text-cube-ok ring-cube-ok/30'
                  : status === 'connecting'
                    ? 'bg-cube-warn/10 text-cube-warn ring-cube-warn/30'
                    : 'bg-cube-err/10 text-cube-err ring-cube-err/30',
              )}
            >
              {status === 'connecting' && <Loader2 size={11} className="animate-spin" />}
              {status === 'ready' ? t('status.ready') : statusText[status]}
            </span>
            <div className="flex-1" />
            <div className="flex items-center gap-1">
              <Button
                size="icon"
                variant="ghost"
                title={t('fontSizeDecrease')}
                onClick={decreaseFontSize}
              >
                <Minus size={14} />
              </Button>
              <span className="w-8 text-center text-xs text-muted-foreground text-num">
                {fontSize}
              </span>
              <Button
                size="icon"
                variant="ghost"
                title={t('fontSizeIncrease')}
                onClick={increaseFontSize}
              >
                <Plus size={14} />
              </Button>
              <Button
                size="icon"
                variant="ghost"
                title={t('fullscreen')}
                onClick={() => setFullscreen((f) => !f)}
              >
                {fullscreen ? <Minimize2 size={14} /> : <Maximize2 size={14} />}
              </Button>
              <Dialog.Close asChild>
                <Button size="icon" variant="ghost" title={t('close')}>
                  <X size={14} />
                </Button>
              </Dialog.Close>
            </div>
          </div>

          {/* Terminal */}
          <div className="relative flex-1 bg-[#0b0e14] p-2">
            <div ref={containerRef} className="h-full w-full" />
            {status !== 'ready' && (
              <div className="absolute inset-0 flex flex-col items-center justify-center gap-3 bg-background/60 backdrop-blur-[1px]">
                {status === 'connecting' ? (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 size={15} className="animate-spin" />
                    {t('status.connecting')}
                  </div>
                ) : (
                  <>
                    <p className="text-sm text-muted-foreground">{statusText[status]}</p>
                    <Button size="sm" variant="outline" onClick={reconnect}>
                      <RotateCcw size={13} /> {t('reconnect')}
                    </Button>
                  </>
                )}
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="border-t border-border/60 px-4 py-2 text-[11px] text-muted-foreground/70">
            {t('pasteHint')}
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
