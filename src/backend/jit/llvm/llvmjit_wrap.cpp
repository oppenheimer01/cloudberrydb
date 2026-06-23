/*-------------------------------------------------------------------------
 *
 * llvmjit_wrap.cpp
 *	  Parts of the LLVM interface not (yet) exposed to C.
 *
 * Copyright (c) 2016-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/llvm/llvmjit_wrap.cpp
 *
 *-------------------------------------------------------------------------
 */

extern "C"
{
#include "postgres.h"
}

#include <llvm-c/Core.h>
#include <llvm/IR/Function.h>
<<<<<<< HEAD
#if LLVM_VERSION_MAJOR < 17
#include <llvm/MC/SubtargetFeature.h>
#endif
#if LLVM_VERSION_MAJOR > 16
#include <llvm/TargetParser/Host.h>
#else
#include <llvm/Support/Host.h>
#endif
=======
>>>>>>> REL_18_BETA1_branch

#include "jit/llvmjit.h"
#include "jit/llvmjit_backport.h"

#ifdef USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include "jit/SectionMemoryManager.h"
#include <llvm/Support/CBindingWrapping.h>
#endif


/*
 * C-API extensions.
 */

LLVMTypeRef
LLVMGetFunctionReturnType(LLVMValueRef r)
{
	return llvm::wrap(llvm::unwrap<llvm::Function>(r)->getReturnType());
}

LLVMTypeRef
<<<<<<< HEAD
LLVMGetFunctionReturnType(LLVMValueRef r)
{
	return llvm::wrap(llvm::unwrap<llvm::Function>(r)->getReturnType());
}

LLVMTypeRef
=======
>>>>>>> REL_18_BETA1_branch
LLVMGetFunctionType(LLVMValueRef r)
{
	return llvm::wrap(llvm::unwrap<llvm::Function>(r)->getFunctionType());
}

<<<<<<< HEAD
#if LLVM_VERSION_MAJOR < 8
LLVMTypeRef
LLVMGlobalGetValueType(LLVMValueRef g)
{
	return llvm::wrap(llvm::unwrap<llvm::GlobalValue>(g)->getValueType());
}
#endif

=======
>>>>>>> REL_18_BETA1_branch
#ifdef USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER
DEFINE_SIMPLE_CONVERSION_FUNCTIONS(llvm::orc::ExecutionSession, LLVMOrcExecutionSessionRef)
DEFINE_SIMPLE_CONVERSION_FUNCTIONS(llvm::orc::ObjectLayer, LLVMOrcObjectLayerRef);

LLVMOrcObjectLayerRef
LLVMOrcCreateRTDyldObjectLinkingLayerWithSafeSectionMemoryManager(LLVMOrcExecutionSessionRef ES)
{
	return wrap(new llvm::orc::RTDyldObjectLinkingLayer(
		*unwrap(ES), [] { return std::make_unique<llvm::backport::SectionMemoryManager>(nullptr, true); }));
}
#endif
